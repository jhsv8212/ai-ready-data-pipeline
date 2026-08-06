"""파이프라인 설정값 중앙 관리 모듈

모든 하드코딩 값을 이 파일에서 관리합니다.
파이프라인 파일에서 import config 로 사용합니다.
"""

# =============================================================================
# Bronze Layer
# =============================================================================

# S3 Landing Zone 기본 경로 (spark.conf.get의 기본값으로 사용)
S3_LANDING_PATH_DEFAULT = (
    "s3://databricks-storage-7474657118263619/"
    "unity-catalog/7474657118263619/landing/documents/"
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
VS_INDEX_NAME = "developer팀.default.silver_document_chunks_index"

# 임베딩 모델 엔드포인트 (Delta Sync 자동 임베딩용)
# Databricks 내장 모델 또는 Model Serving 엔드포인트명
EMBEDDING_MODEL_ENDPOINT = "databricks-gte-large-en"

# 임베딩 대상 컨럼명
EMBEDDING_SOURCE_COLUMN = "chunk_text"

# 소스 테이블 (Vector Search Delta Sync 대상)
VS_SOURCE_TABLE = "developer팀.default.silver_document_chunks"

# 검색 시 반환할 최대 결과 수
VS_NUM_RESULTS = 5

# RAG 응답 생성용 LLM (기존 LLM_MODEL_NAME 재사용 가능)
RAG_LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
