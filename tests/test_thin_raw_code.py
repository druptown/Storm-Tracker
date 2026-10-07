def test_code_digest_tracks_actual_weather_code_not_credentials(tmp_path):
    from storm_tracker_worker.thin_raw_code import source_tree_digest
    (tmp_path/'worker.py').write_bytes(b'weather-code-v1')
    (tmp_path/'settings.json').write_bytes(b'password-must-not-be-read')
    first = source_tree_digest(tmp_path)
    (tmp_path/'settings.json').write_bytes(b'other-private-password')
    assert source_tree_digest(tmp_path) == first
    (tmp_path/'worker.py').write_bytes(b'weather-code-v2')
    assert source_tree_digest(tmp_path) != first
