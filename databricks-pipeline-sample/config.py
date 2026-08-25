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
# Gold Layer - AI Summary (OpenAI 외부 API) — 추후 API 키 발급 후 활성화
# =============================================================================

# OpenAI API 키 (Databricks Secrets 권장)
# spark.conf.set("openai_api_key", dbutils.secrets.get(scope=OPENAI_API_KEY_SCOPE, key=OPENAI_API_KEY_KEY))
OPENAI_API_KEY_SCOPE = "your-scope"  # TODO: 실제 scope로 변경
OPENAI_API_KEY_KEY = "openai-api-key"  # TODO: 실제 key로 변경

# OpenAI 모델명
OPENAI_MODEL_NAME = "gpt-4o"

# OpenAI 출력 최대 토큰 수
OPENAI_MAX_TOKENS = 300

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
# Databricks 내장 모델 또는 Model Serving 엔드포인트명
EMBEDDING_MODEL_ENDPOINT = "databricks-qwen3-embedding-0-6b"

# 임베딩 대상 컨럼명
EMBEDDING_SOURCE_COLUMN = "chunk_content"

# 소스 테이블 (Vector Search Delta Sync 대상)
VS_SOURCE_TABLE = "dev_haesung.gold.gold_document_embeddings"

# 검색 시 반환할 최대 결과 수
VS_NUM_RESULTS = 5

# RAG 응답 생성용 LLM (기존 LLM_MODEL_NAME 재사용 가능)
RAG_LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
