from dataclasses import dataclass

from storm_tracker_worker.calibration import calibration_operational


@dataclass
class Job:
    payload: dict


def _frame(source, points):
    return [
        "region-1@key", source, 100, 6000.0, 6010.0, 0.1,
        len(points), len(points), 3.0, 50.0, 6.0, 53.0, 1.0,
        [[row, column, 5.0, 0.9, 1] for row, column in points],
    ]


def test_worker_calibration_compares_same_minute_provider_frames():
    result = calibration_operational(Job({"frames": [
        _frame("kmi", [(1, 1), (2, 2)]),
        _frame("opera", [(1, 1), (3, 3)]),
    ]}))
    assert result["method"] == "worker_radar_calibration_v1"
    assert len(result["comparisons"]) == 1
    comparison = result["comparisons"][0]
    assert comparison[5:10] == [2, 2, 1, 1, 1]
    assert comparison[10:13] == [0.5, 0.5, 0.5]
