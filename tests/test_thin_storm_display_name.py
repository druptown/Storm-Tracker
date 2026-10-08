from dataclasses import replace

import pytest

from test_thin_operational_runtime import ZeroRainProviders, _home_state
from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.thin_nowcast import StormCell
from storm_tracker_worker.thin_operational_runtime import ThinOperationalRuntime
from storm_tracker_worker.thin_region_planner import ThinRegionPlanner


@pytest.mark.asyncio
async def test_long_cell_identity_does_not_abort_snapshot_or_lose_identity():
    state = parse_desired_state(_home_state())
    runtime = ThinOperationalRuntime(provider_runtime=ZeroRainProviders(), clock=lambda:100.)
    runtime.apply_desired_state(state.as_dict())
    result = await runtime.run_cycle()
    cell_id = 'knmi:region:' + 'a'*160
    cell = StormCell(cell_id,'knmi_radar',100.,51.05,4.42,{},.2,None,None,
                     10.,20.,22.36,26.56,True,region_id='region:home')
    nowcast = replace(result.nowcast,storm_cells=(cell,))
    snapshot = runtime._build_snapshot(result.fusion,nowcast,result.postgis,
        state=state,region_plan=ThinRegionPlanner().plan(state,now=100.))
    storm = snapshot['ui_storms']['items'][0]
    assert storm['storm_id'] == cell_id
    assert len(storm['display_name']) <= 160
    assert storm['display_name'].endswith('…')
    assert storm['motion_speed_kmh'] == 22.36
