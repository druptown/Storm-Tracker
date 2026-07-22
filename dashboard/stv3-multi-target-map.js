
class Stv3MultiTargetMap extends HTMLElement {
  setConfig(config) {
    this.config = { height: 560, zoom: 7, show_lightning: true, distance_rings: [50,100,150,250], ...config };
    if (!this.shadowRoot) this.attachShadow({mode:'open'});
    this._zoom = this.config.zoom;
    this._showLightning = this.config.show_lightning !== false;
    this._showTechnical = this.config.show_technical === true;
  }
  set hass(hass) {
    this._hass = hass;
    const state = hass.states[this.config.entity || 'sensor.stv3_kaart_geojson'];
    const marker = state ? state.state + ':' + state.last_updated : 'none';
    if (marker !== this._marker) { this._marker = marker; this._load(); }
  }
  getCardSize() { return 8; }
  async _load() {
    if (!this._hass) return;
    try {
      this._data = await this._hass.callApi('GET','storm_tracker_v3/geojson');
      const targets = this._data.features.filter(f => f.properties.layer === 'target');
      if (!this._selected || !targets.some(f => f.properties.target_id === this._selected)) {
        this._selected = (targets.find(f => f.properties.primary) || targets[0])?.properties.target_id;
      }
      this._render();
    } catch (error) {
      this.shadowRoot.innerHTML = '<ha-card><div class="error">Kaartfeed kon niet worden geladen: '+this._escape(error.message || error)+'</div></ha-card>';
    }
  }
  _escape(value) {
    return String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }
  _world(lon,lat,z) {
    const scale=256*Math.pow(2,z);
    const safe=Math.max(-85.0511,Math.min(85.0511,lat))*Math.PI/180;
    return {x:(lon+180)/360*scale,y:(1-Math.log(Math.tan(safe)+1/Math.cos(safe))/Math.PI)/2*scale};
  }
  _screen(coord,center,w,h) {
    const p=this._world(coord[0],coord[1],this._zoom);
    return [p.x-center.x+w/2,p.y-center.y+h/2];
  }
  _render() {
    const targets=this._data.features.filter(f=>f.properties.layer==='target');
    const selected=targets.find(f=>f.properties.target_id===this._selected) || targets[0];
    if(!selected) return;
    this.shadowRoot.innerHTML =
      '<style>'+
      ':host{display:block} ha-card{overflow:hidden} .top{display:flex;gap:8px;align-items:center;padding:12px 14px;background:var(--ha-card-background,var(--card-background-color));flex-wrap:wrap}'+
      '.title{font-weight:700;font-size:18px;flex:1;min-width:180px}.controls{display:flex;gap:6px;align-items:center} select,button{font:inherit;color:var(--primary-text-color);background:var(--secondary-background-color);border:1px solid var(--divider-color);border-radius:8px;padding:7px 9px}button{width:36px;font-weight:700;cursor:pointer}.lightning-toggle,.technical-toggle{width:auto;font-size:13px}.lightning-toggle.active{background:#5d4037;color:#fff;border-color:#ffca28}.technical-toggle.active{background:#37474f;color:#fff}'+
      '.map{position:relative;overflow:hidden;background:#cfe7f5;height:'+Number(this.config.height)+'px}.tiles,.overlay{position:absolute;inset:0}.tiles img{position:absolute;width:256px;height:256px}.overlay{pointer-events:none}@keyframes stormPulse{0%,100%{fill-opacity:0;stroke-opacity:0}50%{fill-opacity:.5;stroke-opacity:.5}}.storm-pulse{animation:stormPulse 1.25s ease-in-out infinite}.legend{position:absolute;left:10px;bottom:10px;background:rgba(20,25,30,.82);color:#fff;border-radius:8px;padding:7px 10px;font-size:12px;line-height:1.6}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px}.meta{padding:8px 14px;font-size:12px;color:var(--secondary-text-color)}.error{padding:18px;color:var(--error-color)}'+
      '</style><ha-card><div class="top"><div class="title">Neerslag, bliksem en targets</div><div class="controls"><select aria-label="Target"></select><button class="lightning-toggle'+(this._showLightning?' active':'')+'" title="Bliksemdetail wisselen">&#9889; '+(this._showLightning?'inslagen':'buien')+'</button><button class="technical-toggle'+(this._showTechnical?' active':'')+'" title="Technische contouren">techniek</button><button class="minus" title="Uitzoomen">-</button><button class="plus" title="Inzoomen">+</button></div></div><div class="map"><div class="tiles"></div><svg class="overlay"></svg><div class="legend"><span class="dot" style="background:#00e676"></span>target<br><span class="dot" style="background:#b3e5fc"></span>zeer licht <span class="dot" style="background:#29b6f6"></span>licht <span class="dot" style="background:#1565c0"></span>matig <span class="dot" style="background:#ffee58"></span>fors <span class="dot" style="background:#ff9800"></span>zwaar <span class="dot" style="background:#f44336"></span>zeer zwaar<br><span style="display:inline-block;width:12px;border-top:1px dashed #455a64;vertical-align:middle"></span> afstandsringen<br>'+(this._showLightning?'<span style="color:#ffeb3b;font-size:15px">&#9889;</span> inslagen (geclusterd)':'<span style="color:#ffca28">&#9679;</span> knipperende bui = actief onweer')+'</div></div><div class="meta"></div></ha-card>';
    const select=this.shadowRoot.querySelector('select');
    for(const target of targets){
      const option=document.createElement('option');
      option.value=target.properties.target_id;
      option.textContent=(target.properties.primary?'Home: ':'Target: ')+target.properties.name;
      option.selected=option.value===this._selected;
      select.appendChild(option);
    }
    select.addEventListener('change',e=>{this._selected=e.target.value;this._render();});
    this.shadowRoot.querySelector('.lightning-toggle').addEventListener('click',()=>{this._showLightning=!this._showLightning;this._render();});
    this.shadowRoot.querySelector('.technical-toggle').addEventListener('click',()=>{this._showTechnical=!this._showTechnical;this._render();});
    this.shadowRoot.querySelector('.minus').addEventListener('click',()=>{this._zoom=Math.max(4,this._zoom-1);this._render();});
    this.shadowRoot.querySelector('.plus').addEventListener('click',()=>{this._zoom=Math.min(11,this._zoom+1);this._render();});
    requestAnimationFrame(()=>this._draw(selected));
  }
  _draw(selected) {
    const map=this.shadowRoot.querySelector('.map'), tiles=this.shadowRoot.querySelector('.tiles'), svg=this.shadowRoot.querySelector('.overlay');
    const w=map.clientWidth,h=map.clientHeight,center=this._world(selected.geometry.coordinates[0],selected.geometry.coordinates[1],this._zoom);
    const n=Math.pow(2,this._zoom),minX=Math.floor((center.x-w/2)/256),maxX=Math.floor((center.x+w/2)/256),minY=Math.floor((center.y-h/2)/256),maxY=Math.floor((center.y+h/2)/256);
    for(let x=minX;x<=maxX;x++) for(let y=minY;y<=maxY;y++){
      if(y<0||y>=n) continue;
      const img=document.createElement('img'),wrapped=((x%n)+n)%n;
      img.src='https://tile.openstreetmap.org/'+this._zoom+'/'+wrapped+'/'+y+'.png';
      img.alt=''; img.referrerPolicy='no-referrer';
      img.style.left=(x*256-center.x+w/2)+'px'; img.style.top=(y*256-center.y+h/2)+'px';
      tiles.appendChild(img);
    }
    svg.setAttribute('viewBox','0 0 '+w+' '+h); svg.setAttribute('width',w); svg.setAttribute('height',h);
    const selectedEngine=selected.properties.region_engine;
    const radarOverlay=(this._data.radar_overlays||{})[selectedEngine];
    const visible=this._data.features.filter(f=>{
      if(!selectedEngine) return true;
      if(f.properties.layer==='target') return f.properties.region_engine===selectedEngine;
      return f.properties.engine_id===selectedEngine;
    });
    const allLightning=visible.filter(f=>f.properties.layer==='lightning');
    const filtered=visible.filter(f=>{
      if(!this._showLightning&&f.properties.layer==='lightning') return false;
      if(radarOverlay&&!this._showTechnical&&['storm','radar_cell'].includes(f.properties.layer)) return false;
      return true;
    });
    const order={region:0,storm:1,radar_cell:2,motion:3,lightning:4,target:5};
    const ordered=[...filtered].sort((a,b)=>(order[a.properties.layer]??9)-(order[b.properties.layer]??9));
    const lightning=allLightning;
    if(radarOverlay) this._radarOverlay(svg,radarOverlay,center,w,h,lightning,!this._showLightning);
    this._distanceRings(svg,selected,center,w,h);
    for(const f of ordered) if(!['target','lightning'].includes(f.properties.layer)) this._feature(svg,f,center,w,h);
    if(this._showLightning) this._lightningClusters(svg,lightning,center,w,h);
    this._targetGroups(svg,ordered.filter(f=>f.properties.layer==='target'),center,w,h);
    const availableCells=visible.filter(f=>f.properties.layer==='radar_cell').length;
    const visibleLightning=lightning.length;
    const source=selected.properties.radar_source||'geen';
    const reason=selected.properties.radar_source_reason||'nog niet geselecteerd';
    const goes=selected.properties.goes_rrqpe;
    let goesText='';
    if(goes&&goes.supported){
      const satellites=(goes.satellites||[]).map(n=>'GOES-'+n).join('/');
      const label=satellites||'GOES';
      if(goes.status==='error') goesText=' | '+label+': fout';
      else if(Number(goes.observations||0)>0) goesText=' | '+label+': '+goes.observations+' regencellen';
      else if(goes.status==='active') goesText=' | '+label+': geen echo gedetecteerd';
      else goesText=' | '+label+': '+(goes.status||'standby');
    }
    const overlayText=radarOverlay?' | raster: '+radarOverlay.runs.length+' pixelruns':'';
    this.shadowRoot.querySelector('.meta').textContent=(selected.properties.name||selected.properties.target_id)+' | '+(selectedEngine||'alle engines')+' | radar: '+source+' | '+reason+goesText+overlayText+' | zoom '+this._zoom+' | '+availableCells+' analysecellen | '+visibleLightning+' bliksems (15 min)';
  }
  _radarOverlay(svg,overlay,center,w,h,lightning,pulseStorms) {
    const ns='http://www.w3.org/2000/svg';
    const colors=['transparent','#b3e5fc','#81d4fa','#29b6f6','#1565c0','#ffee58','#ff9800','#f44336','#8e24aa'];
    const paths=new Map(),pulsePaths=[];
    const strikePoints=(lightning||[]).filter(f=>Number(f.properties.age_seconds||0)<300).map(f=>this._screen(f.geometry.coordinates,center,w,h));
    for(const run of overlay.runs||[]){
      const level=Math.max(1,Math.min(8,Number(run.intensity)||1));
      const points=(run.ring||[]).map(c=>this._screen([Number(c[1]),Number(c[0])],center,w,h));
      if(points.length!==4) continue;
      const d=points.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1)).join(' ')+' Z';
      paths.set(level,(paths.get(level)||'')+d+' ');
      if(pulseStorms&&strikePoints.some(s=>{const cx=points.reduce((n,p)=>n+p[0],0)/4,cy=points.reduce((n,p)=>n+p[1],0)/4;return Math.hypot(s[0]-cx,s[1]-cy)<24;})) pulsePaths.push(d);
    }
    for(const [level,d] of [...paths.entries()].sort((a,b)=>a[0]-b[0])){
      const path=document.createElementNS(ns,'path');
      path.setAttribute('d',d);path.setAttribute('fill',colors[level]);path.setAttribute('fill-opacity','.72');path.setAttribute('stroke','none');
      svg.appendChild(path);
    }
    if(pulsePaths.length){const pulse=document.createElementNS(ns,'path');pulse.setAttribute('d',pulsePaths.join(' '));pulse.setAttribute('fill','#ffca28');pulse.setAttribute('stroke','#ff6f00');pulse.setAttribute('stroke-width','2');pulse.setAttribute('class','storm-pulse');svg.appendChild(pulse);}
  }
  _lightningClusters(svg,features,center,w,h){
    const ns='http://www.w3.org/2000/svg',groups=new Map();
    for(const f of features){const p=this._screen(f.geometry.coordinates,center,w,h),key=Math.round(p[0]/18)+','+Math.round(p[1]/18);if(!groups.has(key))groups.set(key,[]);groups.get(key).push({f,p});}
    for(const group of groups.values()){const newest=group.reduce((a,b)=>Number(a.f.properties.age_seconds||0)<Number(b.f.properties.age_seconds||0)?a:b),age=Number(newest.f.properties.age_seconds||0),x=group.reduce((n,v)=>n+v.p[0],0)/group.length,y=group.reduce((n,v)=>n+v.p[1],0)/group.length,color=age<120?'#ffeb3b':age<300?'#ff9800':'#9e9e9e';const bolt=document.createElementNS(ns,'path');bolt.setAttribute('d','M'+(x-2)+' '+(y-7)+'L'+(x+4)+' '+(y-7)+'L'+x+' '+(y-1)+'L'+(x+5)+' '+(y-1)+'L'+(x-4)+' '+(y+8)+'L'+(x-1)+' '+y+'L'+(x-6)+' '+y+'Z');bolt.setAttribute('fill',color);bolt.setAttribute('stroke','#5d4037');bolt.setAttribute('stroke-width','1');svg.appendChild(bolt);if(group.length>1){const t=document.createElementNS(ns,'text');t.setAttribute('x',x+6);t.setAttribute('y',y-5);t.setAttribute('font-size','10');t.setAttribute('font-weight','700');t.setAttribute('fill','#111');t.setAttribute('stroke','#fff');t.setAttribute('stroke-width','3');t.setAttribute('paint-order','stroke');t.textContent=group.length;svg.appendChild(t);}}
  }
  _distanceRings(svg,selected,center,w,h) {
    const ns='http://www.w3.org/2000/svg';
    const coord=selected.geometry.coordinates,origin=this._screen(coord,center,w,h);
    const lat=Number(coord[1]);
    const kmPerLonDegree=Math.max(1,111.32*Math.cos(lat*Math.PI/180));
    const rings=Array.isArray(this.config.distance_rings)?this.config.distance_rings:[50,100,150,250];
    for(const rawDistance of rings){
      const distance=Number(rawDistance);
      if(!Number.isFinite(distance)||distance<=0) continue;
      const edge=this._screen([Number(coord[0])+distance/kmPerLonDegree,lat],center,w,h);
      const radius=Math.abs(edge[0]-origin[0]);
      const circle=document.createElementNS(ns,'circle');
      circle.setAttribute('cx',origin[0]);circle.setAttribute('cy',origin[1]);circle.setAttribute('r',radius);
      circle.setAttribute('fill','none');circle.setAttribute('stroke','#455a64');circle.setAttribute('stroke-width','1');circle.setAttribute('stroke-dasharray','5 4');circle.setAttribute('stroke-opacity','.72');svg.appendChild(circle);
      const label=document.createElementNS(ns,'text');
      label.setAttribute('x',origin[0]+4);label.setAttribute('y',origin[1]-radius+13);label.setAttribute('fill','#263238');label.setAttribute('stroke','#fff');label.setAttribute('stroke-width','3');label.setAttribute('paint-order','stroke');label.setAttribute('font-size','11');label.textContent=distance+' km';svg.appendChild(label);
    }
  }
  _targetGroups(svg,targets,center,w,h) {
    const groups=new Map();
    for(const target of targets){
      const c=target.geometry.coordinates,key=c[0].toFixed(3)+','+c[1].toFixed(3);
      if(!groups.has(key)) groups.set(key,[]);
      groups.get(key).push(target);
    }
    const ns='http://www.w3.org/2000/svg';
    for(const group of groups.values()){
      const active=group.find(f=>f.properties.target_id===this._selected)||group[0];
      const p=this._screen(active.geometry.coordinates,center,w,h),circle=document.createElementNS(ns,'circle');
      circle.setAttribute('cx',p[0]);circle.setAttribute('cy',p[1]);circle.setAttribute('r',active.properties.target_id===this._selected?'8':'6');
      circle.setAttribute('fill','#00e676');circle.setAttribute('stroke','#fff');circle.setAttribute('stroke-width','2');svg.appendChild(circle);
      const label=document.createElementNS(ns,'text');label.setAttribute('x',p[0]+10);label.setAttribute('y',p[1]-8);label.setAttribute('fill','#111');label.setAttribute('stroke','#fff');label.setAttribute('stroke-width','3');label.setAttribute('paint-order','stroke');label.setAttribute('font-size','12');
      label.textContent=active.properties.name+(group.length>1?' +'+(group.length-1):'');svg.appendChild(label);
    }
  }
  _feature(svg,f,center,w,h) {
    const ns='http://www.w3.org/2000/svg',layer=f.properties.layer,type=f.geometry.type;
    const age=Number(f.properties.age_seconds||0),lightningColor=age<120?'#ffeb3b':age<300?'#ff9800':'#9e9e9e';
    const color={target:'#00e676',region:'#ab47bc',storm:'#ff9800',radar_cell:'#2196f3',motion:'#ef5350',lightning:lightningColor}[layer]||'#fff';
    if(type==='Point'){
      const p=this._screen(f.geometry.coordinates,center,w,h);
      if(layer==='lightning'){
        const bolt=document.createElementNS(ns,'path'),x=p[0],y=p[1];
        bolt.setAttribute('d','M'+(x-2)+' '+(y-8)+' L'+(x+4)+' '+(y-8)+' L'+(x+1)+' '+(y-2)+' L'+(x+6)+' '+(y-2)+' L'+(x-4)+' '+(y+9)+' L'+(x-1)+' '+(y+1)+' L'+(x-6)+' '+(y+1)+' Z');bolt.setAttribute('fill',color);bolt.setAttribute('stroke','#5d4037');bolt.setAttribute('stroke-width','1');
        const title=document.createElementNS(ns,'title');title.textContent=(f.properties.source||'bliksem')+' | '+Math.round(age/60)+' min oud';bolt.appendChild(title);svg.appendChild(bolt);return;
      }
      const c=document.createElementNS(ns,'circle');
      c.setAttribute('cx',p[0]);c.setAttribute('cy',p[1]);c.setAttribute('r',layer==='target'?(f.properties.target_id===this._selected?8:5):layer==='region'?7:4);
      c.setAttribute('fill',color);c.setAttribute('fill-opacity',layer==='region'?.75:.95);c.setAttribute('stroke','#fff');c.setAttribute('stroke-width','2');svg.appendChild(c);
      if(layer==='target'){
        const t=document.createElementNS(ns,'text');t.setAttribute('x',p[0]+9);t.setAttribute('y',p[1]-7);t.setAttribute('fill','#111');t.setAttribute('stroke','#fff');t.setAttribute('stroke-width','3');t.setAttribute('paint-order','stroke');t.setAttribute('font-size','12');t.textContent=f.properties.name;svg.appendChild(t);
      }
      return;
    }
    const rings=type==='MultiPolygon'?f.geometry.coordinates.map(p=>p[0]):[type==='Polygon'?f.geometry.coordinates[0]:f.geometry.coordinates];
    if(!rings[0]?.length) return;
    const path=document.createElementNS(ns,'path'),d=rings.map(coords=>coords.map((c,i)=>{const p=this._screen(c,center,w,h);return(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1);}).join(' ')+(type==='Polygon'||type==='MultiPolygon'?' Z':'')).join(' ');
    path.setAttribute('d',d);path.setAttribute('stroke',color);path.setAttribute('stroke-width',layer==='storm'?'3':layer==='motion'?'3':'1.5');path.setAttribute('fill',type==='Polygon'||type==='MultiPolygon'?color:'none');path.setAttribute('fill-opacity',layer==='storm'?'.12':'.20');path.setAttribute('stroke-opacity',layer==='radar_cell'?'.75':'.95');svg.appendChild(path);
  }
}
if(!customElements.get('stv3-multi-target-map')) customElements.define('stv3-multi-target-map',Stv3MultiTargetMap);
window.customCards=window.customCards||[];
window.customCards.push({type:'stv3-multi-target-map',name:'STV3 Multi-target Map',description:'GeoJSON-kaart voor Storm Tracker V3'});

