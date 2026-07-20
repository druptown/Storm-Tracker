
class Stv3MultiTargetMap extends HTMLElement {
  setConfig(config) {
    this.config = { height: 560, zoom: 7, ...config };
    if (!this.shadowRoot) this.attachShadow({mode:'open'});
    this._zoom = this.config.zoom;
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
      '.title{font-weight:700;font-size:18px;flex:1;min-width:180px}.controls{display:flex;gap:6px;align-items:center} select,button{font:inherit;color:var(--primary-text-color);background:var(--secondary-background-color);border:1px solid var(--divider-color);border-radius:8px;padding:7px 9px}button{width:36px;font-weight:700;cursor:pointer}'+
      '.map{position:relative;overflow:hidden;background:#cfe7f5;height:'+Number(this.config.height)+'px}.tiles,.overlay{position:absolute;inset:0}.tiles img{position:absolute;width:256px;height:256px}.overlay{pointer-events:none}.legend{position:absolute;left:10px;bottom:10px;background:rgba(20,25,30,.82);color:#fff;border-radius:8px;padding:7px 10px;font-size:12px;line-height:1.6}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px}.meta{padding:8px 14px;font-size:12px;color:var(--secondary-text-color)}.error{padding:18px;color:var(--error-color)}'+
      '</style><ha-card><div class="top"><div class="title">Neerslag en targets</div><div class="controls"><select aria-label="Target"></select><button class="minus" title="Uitzoomen">-</button><button class="plus" title="Inzoomen">+</button></div></div><div class="map"><div class="tiles"></div><svg class="overlay"></svg><div class="legend"><span class="dot" style="background:#00e676"></span>target &nbsp; <span class="dot" style="background:#ff9800"></span>weersysteem<br><span class="dot" style="background:#2196f3"></span>radarcel &nbsp; <span class="dot" style="background:#ab47bc"></span>RegionEngine</div></div><div class="meta"></div></ha-card>';
    const select=this.shadowRoot.querySelector('select');
    for(const target of targets){
      const option=document.createElement('option');
      option.value=target.properties.target_id;
      option.textContent=(target.properties.primary?'Home: ':'Target: ')+target.properties.name;
      option.selected=option.value===this._selected;
      select.appendChild(option);
    }
    select.addEventListener('change',e=>{this._selected=e.target.value;this._render();});
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
    const visible=this._data.features.filter(f=>{
      if(!selectedEngine) return true;
      if(f.properties.layer==='target') return f.properties.region_engine===selectedEngine;
      return f.properties.engine_id===selectedEngine;
    });
    const ordered=[...visible].sort((a,b)=>({region:0,storm:1,radar_cell:2,motion:3,target:4}[a.properties.layer]-({region:0,storm:1,radar_cell:2,motion:3,target:4}[b.properties.layer])));
    for(const f of ordered) if(f.properties.layer!=='target') this._feature(svg,f,center,w,h);
    this._targetGroups(svg,ordered.filter(f=>f.properties.layer==='target'),center,w,h);
    const visibleCells=ordered.filter(f=>f.properties.layer==='radar_cell').length;
    const source=selected.properties.radar_source||'geen';
    const reason=selected.properties.radar_source_reason||'nog niet geselecteerd';
    this.shadowRoot.querySelector('.meta').textContent=(selected.properties.name||selected.properties.target_id)+' | '+(selectedEngine||'alle engines')+' | radar: '+source+' | '+reason+' | zoom '+this._zoom+' | '+ordered.length+' features | '+visibleCells+' radarcellen';
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
    const color={target:'#00e676',region:'#ab47bc',storm:'#ff9800',radar_cell:'#2196f3',motion:'#ef5350'}[layer]||'#fff';
    if(type==='Point'){
      const p=this._screen(f.geometry.coordinates,center,w,h),c=document.createElementNS(ns,'circle');
      c.setAttribute('cx',p[0]);c.setAttribute('cy',p[1]);c.setAttribute('r',layer==='target'?(f.properties.target_id===this._selected?8:5):layer==='region'?7:4);
      c.setAttribute('fill',color);c.setAttribute('fill-opacity',layer==='region'?.75:.95);c.setAttribute('stroke','#fff');c.setAttribute('stroke-width','2');svg.appendChild(c);
      if(layer==='target'){
        const t=document.createElementNS(ns,'text');t.setAttribute('x',p[0]+9);t.setAttribute('y',p[1]-7);t.setAttribute('fill','#111');t.setAttribute('stroke','#fff');t.setAttribute('stroke-width','3');t.setAttribute('paint-order','stroke');t.setAttribute('font-size','12');t.textContent=f.properties.name;svg.appendChild(t);
      }
      return;
    }
    const coords=type==='Polygon'?f.geometry.coordinates[0]:f.geometry.coordinates;
    if(!coords?.length) return;
    const path=document.createElementNS(ns,'path'),d=coords.map((c,i)=>{const p=this._screen(c,center,w,h);return(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1);}).join(' ')+(type==='Polygon'?' Z':'');
    path.setAttribute('d',d);path.setAttribute('stroke',color);path.setAttribute('stroke-width',layer==='storm'?'3':layer==='motion'?'3':'1.5');path.setAttribute('fill',type==='Polygon'?color:'none');path.setAttribute('fill-opacity',layer==='storm'?'.12':'.20');path.setAttribute('stroke-opacity',layer==='radar_cell'?'.75':'.95');svg.appendChild(path);
  }
}
if(!customElements.get('stv3-multi-target-map')) customElements.define('stv3-multi-target-map',Stv3MultiTargetMap);
window.customCards=window.customCards||[];
window.customCards.push({type:'stv3-multi-target-map',name:'STV3 Multi-target Map',description:'GeoJSON-kaart voor Storm Tracker V3'});

