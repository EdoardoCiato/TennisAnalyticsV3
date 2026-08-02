from pathlib import Path 

def ensure_dir(path):
    if not path.exists():
        path.mkdir(parents=True)


PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / 'data' / 'db' / "tennis_abstract_new_version_merged_testing.db"
OUTPUT_DIR = PROJECT_ROOT / 'outputs'
ensure_dir(OUTPUT_DIR)

OUTPUT_CHART_DIR = OUTPUT_DIR / 'chart'
OUTPUT_COURT_VISION_DIR = OUTPUT_DIR / 'court_vision'
OUTPUT_EXCEL_DIR = OUTPUT_DIR / 'excel'
OUTPUT_HTML_DIR = OUTPUT_DIR / 'html'
OUTPUT_PDF_DIR = OUTPUT_DIR / 'pdf'

for path in [OUTPUT_CHART_DIR, OUTPUT_COURT_VISION_DIR, OUTPUT_EXCEL_DIR, OUTPUT_HTML_DIR, OUTPUT_PDF_DIR]:
    ensure_dir(path)

TEMPLATES_DIR = PROJECT_ROOT / 'templates'
ensure_dir(TEMPLATES_DIR)

