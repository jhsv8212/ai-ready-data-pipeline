"""파이프라인 설정값 중앙 관리 모듈

모든 하드코딩 값을 이 파일에서 관리합니다.
파이프라인 파일에서 import config 로 사용합니다.
"""

# =============================================================================
# Bronze Layer
# =============================================================================

# S3 Landing Zone 기본 경로 (spark.conf.get의 기본값으로 사용)
S3_LANDING_PATH_DEFAULT = "s3://a-s3-dbx-dev-ane2-aegis01/보험/"


def get_category_list(s3_landing_path: str = None) -> list:
    """S3 Landing Zone 바로 아래 1뎁스 폴더명을 카테고리 목록으로 반환합니다.

    bronze/silver/gold 레이어는 이 목록을 기준으로 카테고리별 테이블
    ({카테고리명}_bronze_documents 등)을 동적으로 생성합니다. 파이프라인
    그래프를 빌드하는 시점(코드 최상위 for 루프)에 호출되므로, 새 카테고리
    폴더가 S3에 추가되면 다음 파이프라인 업데이트/Full Refresh 시
    테이블이 자동으로 새로 생성됩니다.
    """
    path = s3_landing_path or S3_LANDING_PATH_DEFAULT
    entries = dbutils.fs.ls(path)
    # dbutils.fs.ls()가 반환하는 FileInfo는 디렉터리일 때 name이 "/"로 끝난다
    # (isDir() 같은 별도 메서드는 보장되지 않으므로 이 표기 규칙에 의존한다).
    return sorted(
        entry.name.rstrip("/")
        for entry in entries
        if entry.name.endswith("/") and entry.name.rstrip("/")
    )

# =============================================================================
# Silver Layer
# =============================================================================

# ai_parse_document 버전
AI_PARSE_DOCUMENT_VERSION = "2.0"

# =============================================================================
# Gold Layer - Document Summary
# =============================================================================

# 텍스트 미리보기 길이 (글자수)
TEXT_PREVIEW_LENGTH = 500

# =============================================================================
# Gold Layer - AI Summary
# =============================================================================

# LLM 모델명
LLM_MODEL_NAME = "databricks-meta-llama-3-3-70b-instruct"

# AI 요약 프롬프트
AI_SUMMARY_PROMPT = (
    "다음 보험 문서를 한국어로 3~5문장으로 요약하세요. "
    "핵심 보장 내용, 주요 조건, 대상 독자를 포함하세요.\n\n"
)

# AI 입력 텍스트 최대 길이 (글자수)
AI_INPUT_MAX_CHARS = 3000

# AI 출력 최대 토큰 수
AI_MAX_TOKENS = 300

# =============================================================================
# Gold Layer - AI Summary 메타데이터 스키마
# =============================================================================
# dev_haesung_pipeline_architecture.md 문서에 임시로 정의해두었던 metadata JSON 필드를
# 이곳으로 옮겨 관리합니다. 필드를 추가/변경/삭제하려면 아래 METADATA_SCHEMA만 수정하면
# 되며, gold_document_ai_summary.py의 메타데이터 추출 프롬프트는
# build_metadata_prompt_fields()를 통해 이 정의를 그대로 가져다 씁니다(프롬프트 문자열을
# 직접 고칠 필요 없음).
#
# 필드 항목 구조:
#   field: 추출할 JSON 키 이름
#   description: 필드에 대한 한글 설명
#   options: 허용 값 목록 (자유 텍스트 필드는 None)
#   is_array: True면 "중 해당하는 것", False/생략이면 "중 택1" 문구를 붙임
METADATA_SCHEMA = [
    {
        "field": "doc_category",
        "description": "문서 유형",
        "options": ["약관", "사업방법서", "상품설명서", "언더라이팅가이드", "보험금지급기준", "고객안내문"],
    },
    {
        "field": "insurance_product_type",
        "description": "보험 상품 유형",
        "options": ["종신", "정기", "CI", "변액", "연금", "건강", "실손", "해당없음"],
    },
    {
        "field": "related_systems",
        "description": "관련 시스템 배열",
        "options": ["보험코어", "언더라이팅", "보상", "CRM", "전자청약", "계리"],
        "is_array": True,
    },
    {
        "field": "sensitivity_level",
        "description": "민감도",
        "options": ["공개", "내부", "민감", "개인정보포함", "의료정보포함"],
    },
    {
        "field": "regulation_reference",
        "description": "관련 규정 (보험업법 조항, 감독규정 등. 없으면 null)",
        "options": None,
    },
]


def build_metadata_prompt_fields() -> str:
    """METADATA_SCHEMA를 LLM 프롬프트에 삽입할 필드 설명 텍스트로 변환합니다.

    METADATA_SCHEMA를 수정하면 ai_query / 고객사 LLM API 프롬프트에 자동 반영됩니다.
    """
    lines = []
    for field in METADATA_SCHEMA:
        line = f"- {field['field']}: {field['description']}"
        options = field.get("options")
        if options:
            choice_text = "/".join(options)
            suffix = "중 해당하는 것" if field.get("is_array") else "중 택1"
            line += f" ({choice_text} {suffix})"
        lines.append(line)
    return "\n".join(lines)


# =============================================================================
# Gold Layer - AI Summary (OpenAI 외부 API) — 고객사 LLM API 연동 계획으로 대체, 비활성화
# =============================================================================

# --- 아래 OpenAI 연동 설정은 고객사 제공 LLM API로 대체되어 더 이상 사용하지 않습니다. ---
# --- 참고용으로 주석 처리하여 남겨둡니다. 활성 설정은 아래 CLIENT_LLM_API_* 를 참고하세요. ---
#
# OPENAI_API_KEY_SCOPE = "your-scope"
# OPENAI_API_KEY_KEY = "openai-api-key"
# OPENAI_MODEL_NAME = "gpt-4o"
# OPENAI_MAX_TOKENS = 300

# =============================================================================
# Gold Layer - AI Summary (고객사 제공 LLM API) — 고객사 개발/전달 완료 후 연동 예정
# =============================================================================

# 고객사에서 제공하는 LLM API 엔드포인트
# TODO: 고객사로부터 API 전달받은 후 실제 엔드포인트 URL로 변경
CLIENT_LLM_API_URL = "http://TODO-client-llm-api/v1/chat/completions"

# 고객사 LLM API 인증키 (Databricks Secrets 권장)
# 사용 예: dbutils.secrets.get(scope=CLIENT_LLM_API_KEY_SCOPE, key=CLIENT_LLM_API_KEY_KEY)
CLIENT_LLM_API_KEY_SCOPE = "your-scope"  # TODO: 실제 scope로 변경
CLIENT_LLM_API_KEY_KEY = "client-llm-api-key"  # TODO: 실제 key로 변경

# 고객사에서 전달할 모델명
CLIENT_LLM_MODEL_NAME = "TODO"  # TODO: 고객사로부터 전달받은 실제 모델명으로 변경

# 고객사 LLM API 출력 최대 토큰 수
CLIENT_LLM_MAX_TOKENS = 300

# 고객사 LLM API 타임아웃(초)
CLIENT_LLM_API_TIMEOUT = 30

# =============================================================================
# Gold Layer - Document Chunks (시멘틱 청킹)
# =============================================================================

# 시멘틱 청킹에 사용할 LLM 모델
CHUNKING_LLM_MODEL = LLM_MODEL_NAME

# 시멘틱 청킹 시 입력 텍스트 최대 길이 (글자수)
CHUNKING_INPUT_MAX_CHARS = 2000

# 시멘틱 청킹 출력 최대 토큰 수
CHUNKING_MAX_TOKENS = 1500

# 시멘틱 청킹 프롬프트
CHUNKING_PROMPT = (
    "다음 텍스트를 의미 단위로 분할하여 JSON 배열로 반환하세요. "
    "각 청크는 하나의 완결된 의미를 가져야 합니다. "
    '반드시 JSON 배열 형식으로만 응답하세요: ["청크1", "청크2", ...]\n\n'
)

# =============================================================================
# Gold Layer - 경로 기반 청킹 전략 (Path-Based Chunking Strategies)
# =============================================================================

# source_file 경로에 glob 패턴을 매칭하여 해당 청킹 전략 적용
# 패턴 문법:
#   **  → 임의 경로 깊이 (/ 포함, 재귀적 매칭)
#   *   → 단일 디렉토리 내 임의 문자 (/ 제외)
#   ?   → 단일 문자
#
# 청킹 모드 (chunk_mode):
#   "none"     → 청킹 없이 원본 텍스트 그대로 사용 (전체 텍스트 = 1개 청크)
#   "fixed"    → 고정 길이 청킹 (chunk_size 글자수로 잘라서 분할)
#   "overlap"  → 오버랩 청킹 (chunk_size + chunk_overlap 으로 문맥 보존 분할)
#
# 매칭 순서: 위에서부터 순서대로 검사, 첫 매칭 적용. 미매칭 시 default
# TODO: 실제 S3 디렉토리 구조에 맞게 path_pattern을 수정하세요
PATH_CHUNKING_STRATEGIES = {
    # -------------------------------------------------------------------------
    # 1. 청킹 없음 (No Chunking)
    #    원본 텍스트 전체를 하나의 청크로 사용 (짧은 문서, 요약문 등)
    #    예: s3://bucket/landing/documents/summaries/단순요약.pdf
    # -------------------------------------------------------------------------
    "no_chunk": {
        "path_pattern": "**/summaries/**/*",
        "chunk_mode": "none",
    },
    # -------------------------------------------------------------------------
    # 2. 고정 길이 청킹 (Fixed-Length Chunking)
    #    텍스트를 chunk_size 글자 단위로 잘라서 분할 (오버랩 없음)
    #    예: s3://bucket/landing/documents/raw_text/대량문서.pdf
    # -------------------------------------------------------------------------
    "fixed_chunk": {
        "path_pattern": "**/raw_text/**/*.pdf",
        "chunk_mode": "fixed",
        "chunk_size": 500,
    },
    # -------------------------------------------------------------------------
    # 3. 오버랩 청킹 (Overlap Chunking)
    #    chunk_size로 잘되, 앞뒤 청크간 chunk_overlap만큼 문맥 공유
    #    예: s3://bucket/landing/documents/contracts/계약서.pdf
    # -------------------------------------------------------------------------
    "overlap_chunk": {
        "path_pattern": "**/contracts/**/*.pdf",
        "chunk_mode": "overlap",
        "chunk_size": 500,
        "chunk_overlap": 100,
    },
    # -------------------------------------------------------------------------
    # 기본 (fallback): 위 패턴 어느 것에도 매칭되지 않는 모든 파일
    #    청킹 없이 원본 그대로 사용
    # -------------------------------------------------------------------------
    "default": {
        "path_pattern": None,
        "chunk_mode": "none",
    },
}

# =============================================================================
# Silver Layer - Document Chunks (RAG 청킹)
# =============================================================================

# 청크 크기 (글자수) — 임베딩 모델 토큰 제한에 맞춰 조정
CHUNK_SIZE = 500

# 청크 간 중복 (글자수) — 문맥 유지를 위한 오버랩
CHUNK_OVERLAP = 100

# =============================================================================
# Vector Search (RAG)
# =============================================================================

# Vector Search 엔드포인트명
VS_ENDPOINT_NAME = "document-search-endpoint"

# Vector Search 인덱스명
VS_INDEX_NAME = "dev_haesung.gold.gold_document_embeddings_index"

# 임베딩 모델 엔드포인트 (Delta Sync 자동 임베딩용)
# --- bge-m3 FastAPI 서비스 연동 계획으로 대체되어 더 이상 사용하지 않습니다. ---
# EMBEDDING_MODEL_ENDPOINT = "databricks-qwen3-embedding-0-6b"

# 임베딩 대상 컨럼명
EMBEDDING_SOURCE_COLUMN = "chunk_content"

# 소스 테이블 (Vector Search Delta Sync 대상)
VS_SOURCE_TABLE = "dev_haesung.gold.gold_document_embeddings"

# 검색 시 반환할 최대 결과 수
VS_NUM_RESULTS = 5

# RAG 응답 생성용 LLM (기존 LLM_MODEL_NAME 재사용 가능)
RAG_LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"

# =============================================================================
# Gold Layer - Embedding (외부 bge-m3 FastAPI 서비스) — 다른 팀 개발 완료 후 연동 예정
# =============================================================================

# 다른 팀에서 개발 중인 bge-m3 임베딩 FastAPI 서비스 엔드포인트
# TODO: 다른 팀 개발 완료 후 실제 엔드포인트 URL로 변경
EMBEDDING_API_URL = "http://TODO-bge-m3-embedding-api/embed"

# 임베딩 모델명
EMBEDDING_API_MODEL_NAME = "bge-m3"

# bge-m3 임베딩 차원 (dense vector 기준)
EMBEDDING_API_DIMENSION = 1024

# 임베딩 API 요청 1회당 텍스트 배치 크기
EMBEDDING_API_BATCH_SIZE = 32

# 임베딩 API 타임아웃(초)
EMBEDDING_API_TIMEOUT = 30
