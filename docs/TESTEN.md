# Tests uitvoeren

Gebruik op Windows PowerShell een aparte virtual environment:

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r .\requirements-dev.txt
python -m pip install "h5py>=3.11,<4" "numpy>=2,<3" "pyproj>=3.6,<4" aiohttp
python -m pytest -q
```

De optionele echte OPERA-integratietest:

```powershell
$env:STV3_RUN_INTEGRATION_TESTS="1"
python -m pytest -q .\tests\test_opera_integration.py
```

Deze test gebruikt de openbare OPERA S3-bucket en hoort niet verplicht in CI.
