# 컴퓨트 & AI 검색 콘솔 설정 가이드

이 문서는 `databricks-pipeline-sample` 파이프라인에 필요한 컴퓨트 환경과 AI 검색(Vector Search) 콘솔 설정 방법을 안내합니다.

---

## 목차

1. [컴퓨트 설정](#컴퓨트-설정)
2. [AI 함수 사용을 위한 모델 서빙 확인](#ai-함수-사용을-위한-모델-서빙-확인)
3. [Vector Search 엔드포인트 생성](#vector-search-엔드포인트-생성)
4. [Vector Search 인덱스 생성](#vector-search-인덱스-생성)
5. [검색 테스트](#검색-테스트)
6. [트러블슈팅](#트러블슈팅)

---

## 컴퓨트 설정

### 파이프라인 컴퓨트 (파이프라인 실행용)

이 파이프라인은 **Serverless** 모드로 실행되며 별도 클러스터 설정이 필요 없습니다.

| 항목 | 설정값 | 비고 |
|---|---|---|
| Compute type | **Serverless** | 클러스터 관리 불필요 |
| Photon acceleration | **활성화** | 성능 최적화 |
| Channel | **Current** | 안정 버전 사용 |

#### 콘솔에서 설정하기

1. **Lakeflow** → **Pipelines** → `databricks-pipeline-sample` 선택
2. 우상단 **Settings** 클릭
3. **Compute** 섹션:
   - **Serverless** 선택 (기본 권장)
   - **Photon acceleration** 체크박스 활성화
4. **Save** 클릭

> **참고**: Serverless 모드에서는 `ai_query()`, `ai_parse_document()` 등 AI 함수가 자동으로 지원됩니다. Classic 컴퓨트를 사용하는 경우 DBR 14.3+ 이상이 필요합니다.

### SQL Warehouse (대시보드 / 직접 쿼리용)

대시보드나 SQL 에디터에서 AI 함수를 사용하려면:

1. **SQL Warehouses** → 사용 중인 Warehouse 선택
2. **Type**: Serverless (권장) 또는 Pro
3. AI 함수 사용 조건:
   - Serverless SQL Warehouse: 모든 AI 함수 자동 지원
   - Pro SQL Warehouse: `ai_query()` 지원 (DBR 14.3+)

---

## AI 함수 사용을 위한 모델 서빙 확인

이 파이프라인에서 사용하는 AI 모델들이 활성화되어 있는지 확인합니다.

### 필수 모델 엔드포인트

| 모델 | 용도 | 사용 위치 |
|---|---|---|
| `databricks-meta-llama-3-3-70b-instruct` | 시멘틱 청킹 | gold_document_chunks |
| `databricks-qwen3-embedding-0-6b` | 벡터 임베딩 (Vector Search 자동 계산) | Vector Search 인덱스 |

### 콘솔에서 확인하기

1. 왼쪽 메뉴 **Serving** 클릭 (또는 **Machine Learning** → **Serving**)
2. **Serving endpoints** 목록에서 아래 엔드포인트 확인:
   - `databricks-meta-llama-3-3-70b-instruct` — 상태: **Ready**
   - `databricks-qwen3-embedding-0-6b` — 상태: **Ready**
3. 엔드포인트가 보이지 않으면:
   - Databricks 내장 Foundation Model은 자동 제공됩니다 (pay-per-token)
   - Workspace Admin이 Foundation Model APIs를 비활성화한 경우 활성화 필요

> **설정 경로**: Admin Console → Workspace Settings → **Foundation Model APIs** → 활성화

---

## Vector Search 엔드포인트 생성

Vector Search를 사용하려면 먼저 엔드포인트를 생성해야 합니다.

### 콘솔에서 생성하기

1. 왼쪽 메뉴 **Compute** 클릭
2. 상단 탭에서 **Vector Search** 선택
3. **Create** 버튼 클릭
4. 아래 설정 입력:

| 항목 | 설정값 |
|---|---|
| Endpoint name | `document-search-endpoint` |
| Endpoint type | Standard |

5. **Confirm** 클릭
6. 상태가 **Online** 이 될 때까지 대기 (2~5분 소요)

### Python SDK로 생성하기

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
vsc.create_endpoint(name="document-search-endpoint")
```

---

## Vector Search 인덱스 생성

파이프라인에서 `gold_document_embeddings` 테이블을 생성한 후, 인덱스를 설정합니다.

### 콘솔에서 생성하기

1. **Compute** → **Vector Search** → `document-search-endpoint` 클릭
2. **Create Index** 버튼 클릭
3. 아래 설정 입력:

| 항목 | 설정값 |
|---|---|
| Source table | `dev_haesung.default.gold_document_embeddings` |
| Index name | `dev_haesung.default.gold_document_embeddings_index` |
| Primary key | `chunk_id` |
| Sync mode | **Triggered** (수동 동기화) |
| Embedding source | **Compute embeddings** |
| Embedding model | `databricks-qwen3-embedding-0-6b` |
| Source column | `chunk_content` |

4. **Create** 클릭
5. 상태가 **Online** 이 될 때까지 대기

### Python SDK로 생성하기

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

vsc.create_delta_sync_index(
    endpoint_name="document-search-endpoint",
    index_name="dev_haesung.default.gold_document_embeddings_index",
    source_table_name="dev_haesung.default.gold_document_embeddings",
    pipeline_type="TRIGGERED",
    primary_key="chunk_id",
    embedding_source_columns=[{
        "name": "chunk_content",
        "model_endpoint_name": "databricks-qwen3-embedding-0-6b"
    }],
)
```

### 인덱스 동기화 (파이프라인 실행 후)

파이프라인이 `gold_document_embeddings` 테이블을 업데이트한 후 인덱스를 동기화합니다:

**콘솔에서:**
1. **Compute** → **Vector Search** → `document-search-endpoint` 클릭
2. 인덱스 목록에서 `gold_document_embeddings_index` 선택
3. **Sync now** 버튼 클릭

**Python SDK로:**
```python
vsc.get_index(
    endpoint_name="document-search-endpoint",
    index_name="dev_haesung.default.gold_document_embeddings_index",
).sync()
```

---

## 검색 테스트

인덱스 동기화 완료 후, 유사도 검색을 테스트합니다.

### Python SDK

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
index = vsc.get_index(
    endpoint_name="document-search-endpoint",
    index_name="dev_haesung.default.gold_document_embeddings_index",
)

results = index.similarity_search(
    query_text="보험 보장 내용",
    columns=["chunk_content", "document_id", "element_type"],
    num_results=5,
)

for doc in results["result"]["data_array"]:
    print(doc)
```

### SQL (노트북 / SQL Editor)

```sql
SELECT *
FROM vector_search(
    index => 'dev_haesung.default.gold_document_embeddings_index',
    query => '보험 보장 내용',
    num_results => 5
)
```

---

## 트러블슈팅

### AI 함수 오류

| 증상 | 원인 | 해결 |
|---|---|---|
| `ai_query() is not supported` | Classic compute 사용 중 | Serverless로 변경 또는 DBR 14.3+ 사용 |
| `Model not found` | Foundation Model 비활성화 | Admin Console → Foundation Model APIs 활성화 |
| `Rate limit exceeded` | 토큰 제한 초과 | 입력 텍스트 길이 축소 (`config.py` 수정) |
| `Timeout` | 응답 시간 초과 | `CHUNKING_INPUT_MAX_CHARS` 감소 |

### Vector Search 오류

| 증상 | 원인 | 해결 |
|---|---|---|
| `Endpoint not found` | 엔드포인트 미생성 | 위 "엔드포인트 생성" 절차 수행 |
| `Index out of sync` | 소스 테이블 업데이트 후 미동기화 | **Sync now** 실행 |
| `Dimension mismatch` | 임베딩 차원 불일치 | 인덱스 삭제 후 올바른 모델로 재생성 |
| `Permission denied` | Unity Catalog 권한 부족 | 테이블에 SELECT 권한 부여 |

### 성능 최적화 팁

- **청킹 입력 길이**: `CHUNKING_INPUT_MAX_CHARS`를 줄이면 AI 함수 응답 시간이 단축됩니다
- **임베딩 배치**: 대량 문서 처리 시 Full Refresh로 한 번에 임베딩 생성
- **인덱스 동기화**: Triggered 모드에서 파이프라인 완료 후 수동 Sync 권장
- **검색 품질**: `num_results`를 늘린 후 상위 N개만 필터링하면 Recall 향상

---

## 설정값 요약 (`config.py`)

이 파이프라인의 모든 설정은 `config.py`에서 중앙 관리됩니다.

| 설정값 | 기본값 | 설명 |
|---|---|---|
| `LLM_MODEL_NAME` | `databricks-meta-llama-3-3-70b-instruct` | AI 요약/청킹용 LLM |
| `CHUNKING_LLM_MODEL` | `LLM_MODEL_NAME`과 동일 | 시멘틱 청킹 전용 LLM |
| `CHUNKING_INPUT_MAX_CHARS` | `2000` | 청킹 입력 최대 글자수 |
| `CHUNKING_MAX_TOKENS` | `1500` | 청킹 출력 최대 토큰 |
| `EMBEDDING_MODEL_ENDPOINT` | `databricks-qwen3-embedding-0-6b` | 임베딩 모델 (Vector Search 자동 계산) |
| `VS_ENDPOINT_NAME` | `document-search-endpoint` | Vector Search 엔드포인트명 |
| `VS_NUM_RESULTS` | `5` | 검색 시 반환할 최대 결과 수 |
