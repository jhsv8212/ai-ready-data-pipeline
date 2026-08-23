# databricks-pipeline-sample

S3에 적재된 보험 문서(PDF)를 Auto Loader로 수집하고, Databricks AI 함수로 텍스트를 추출·요약·청킹·임베딩하여 RAG(Retrieval-Augmented Generation) 파이프라인을 구성하는 Lakeflow Spark Declarative Pipeline입니다.

---

## 목차

1. [아키텍처](#아키텍처)
2. [데이터셋](#데이터셋)
3. [파일 구조](#파일-구조)
4. [파이프라인 콘솔 설정 가이드](#파이프라인-콘솔-설정-가이드)
5. [대시보드 콘솔 설정 가이드](#대시보드-콘솔-설정-가이드)
6. [실행 방법](#실행-방법)
7. [자동 트리거](#자동-트리거)
8. [사용 AI 함수](#사용-ai-함수)
9. [Vector Search 연동](#vector-search-연동)

---

## 아키텍처

```
S3 Landing Zone (PDF)
        │
        ▼ Auto Loader (binaryFile, allowOverwrites=true)
┌───────────────────────────────┐
│   staging_documents           │  Streaming Table — 파일 메타데이터 및 버전 이력
│   (dev_haesung.staging)       │  S3 버전 관리, 파일 도착 이력 추적
└───────────────────────────────┘
        │
        ▼ Stream-static join (staging metadata + S3 binary)
┌───────────────────────────────┐
│   bronze_documents            │  Streaming Table — PDF 바이너리 + 메타데이터
│   (dev_haesung.bronze)        │
└───────────────────────────────┘
        │
        ▼ ai_parse_document()
┌───────────────────────────────┐
│   silver_documents            │  Streaming Table — 구조화된 텍스트 + figure 설명 + VARIANT
│   (dev_haesung.silver)        │
└───────────────────────────────┘
        │
        ├───────────────────────────────────────────────────────────┐
        ▼                                                           ▼
┌────────────────────────────────┐          ┌──────────────────────────────────┐
│ gold_document_ai_summary       │          │   silver_document_chunks         │
│ (AI 요약 - ai_query)            │          │ (요소별 오버랩 청킹 - RAG용)      │
│ (dev_haesung.gold)             │          │ (dev_haesung.silver)             │
└────────────────────────────────┘          └──────────────────────────────────┘
                                                        │
                                                        ▼ (Streaming Table)
                                            ┌──────────────────────────────────┐
                                            │  gold_document_embeddings        │
                                            │ (CDF → Vector Search 소스)       │
                                            │ (dev_haesung.gold)               │
                                            └──────────────────────────────────┘
                                                        │
                                                        ▼ Delta Sync
                                            ┌──────────────────────────────────┐
                                            │  Vector Search Index             │
                                            │ (유사도 검색 - RAG 응답 생성)      │
                                            └──────────────────────────────────┘
```

---

## 데이터셋

| 테이블 | 스키마 | 타입 | 설명 |
|---|---|---|---|
| `staging_documents` | `dev_haesung.staging` | Streaming Table | S3 파일 메타데이터 및 버전 이력 (바이너리 미저장) |
| `bronze_documents` | `dev_haesung.bronze` | Streaming Table | S3 PDF 원본 바이너리 + staging 메타데이터 |
| `silver_documents` | `dev_haesung.silver` | Streaming Table | `ai_parse_document()`로 추출한 텍스트·figure 설명·메타데이터 |
| `silver_document_chunks` | `dev_haesung.silver` | Streaming Table | 문서 요소별 오버랩 청킹 (RAG 벡터검색용) |
| `gold_document_ai_summary` | `dev_haesung.gold` | Streaming Table | `ai_query()`로 생성한 문서별 한국어 AI 요약 |
| `gold_document_embeddings` | `dev_haesung.gold` | Streaming Table | Vector Search 소스 테이블 (CDF 활성화) |

---

## 파일 구조

```
databricks-pipeline-sample/
├── README.md
├── config.py                                  # 파이프라인 설정값 중앙 관리 모듈
├── compute-ai-search-setup-guide.md           # Vector Search 설정 가이드
├── file-arrival-trigger-troubleshooting.md    # File Arrival 트리거 트러블슈팅 가이드
├── dev_haesung_default_ERD.py                 # ERD 다이어그램 생성 노트북
├── staging/
│   └── staging__documents.py                  # S3 파일 메타데이터 및 버전 이력 (dev_haesung.staging)
├── bronze/
│   └── bronze_documents.py                    # staging + S3 binary join (dev_haesung.bronze)
├── silver/
│   ├── silver_documents.py                    # ai_parse_document()로 텍스트 추출 (dev_haesung.silver)
│   └── silver_document_chunks.py              # 요소별 오버랩 청킹 (dev_haesung.silver)
└── gold/
    ├── gold_document_ai_summary.py            # AI 요약 (dev_haesung.gold)
    └── gold_document_embeddings.py            # Vector Search 소스 (dev_haesung.gold)
```

---

## 파이프라인 콘솔 설정 가이드

콘솔에서 수동으로 파이프라인을 생성·설정하는 방법입니다.

### 1. 파이프라인 생성

1. 왼쪽 메뉴에서 **Lakeflow** → **Pipelines** 클릭
2. 우상단 **Create pipeline** 버튼 클릭
3. Pipeline name 입력: `databricks-pipeline-sample`

### 2. 소스 파일 추가

1. **Source code** 섹션에서 **Add source code** 클릭
2. 파일 탐색기에서 아래 경로의 폴더 선택 (glob 패턴 자동 적용)
   ```
   /Users/{username}/databricks-pipeline-sample/
   ```
3. 저장 시 glob 패턴이 아래와 같이 설정됨:
   ```
   /Users/{username}/databricks-pipeline-sample/**
   ```

### 3. Catalog / Schema 설정

1. **Destination** 섹션에서 **Storage options** 클릭
2. **Catalog** 항목에 `dev_haesung` 입력
3. **Target schema** 항목에 `default` 입력 (각 테이블이 fully-qualified name으로 스키마를 지정하므로 기본값은 무시됨)

> **참고**: 실제 테이블은 각각 `dev_haesung.staging`, `dev_haesung.bronze`, `dev_haesung.silver`, `dev_haesung.gold` 스키마에 발행됩니다.

### 4. Compute 설정

| 항목 | 설정값 |
|---|---|
| Compute type | Serverless |
| Photon acceleration | 활성화 (체크) |
| Channel | Current |

1. **Compute** 섹션 → **Serverless** 선택
2. **Photon acceleration** 체크박스 활성화

### 5. Configuration 추가 (S3 경로)

1. **Advanced** 섹션 → **Configuration** 클릭
2. **Add configuration** 버튼 클릭
3. 아래 키-값 입력:

| Key | Value |
|---|---|
| `s3_landing_path` | `s3://databricks-storage-7474657118263619/unity-catalog/7474657118263619/landing/documents/` |

> **참고**: 이 값은 `bronze_documents.py`에서 `spark.conf.get("s3_landing_path", ...)` 으로 참조됩니다. S3 경로 변경 시 코드 수정 없이 이 값만 바꾸면 됩니다.

### 6. 파이프라인 저장 및 실행

1. 우상단 **Save** 클릭 후 **Start** 클릭 (일반 업데이트)
2. 스키마 변경 시: **Start** 옆 화살표(▼) → **Full refresh** 선택

---

## 대시보드 콘솔 설정 가이드

콘솔에서 수동으로 AI/BI 대시보드를 생성·설정하는 방법입니다.

### 1. 대시보드 생성

1. 왼쪽 메뉴에서 **SQL** → **Dashboards** 클릭
2. 우상단 **Create dashboard** 버튼 클릭
3. 대시보드 이름 입력: `보험 문서 파이프라인 결과 대시보드`

### 2. 데이터셋 추가

대시보드 편집 화면 하단의 **Data** 탭에서 각 데이터셋을 추가합니다.

**데이터셋 1 — Gold Document AI Summary**

1. **Create dataset** 클릭
2. Dataset name: `Gold Document AI Summary`
3. 아래 SQL 입력 후 **Save**:
   ```sql
   SELECT * FROM `dev_haesung`.`gold`.`gold_document_ai_summary`
   ```

**데이터셋 2 — Gold Document Chunks**

1. **Create dataset** 클릭
2. Dataset name: `Gold Document Chunks`
3. 아래 SQL 입력 후 **Save**:
   ```sql
   SELECT * FROM `dev_haesung`.`silver`.`silver_document_chunks`
   ```

**데이터셋 3 — Gold Document Embeddings**

1. **Create dataset** 클릭
2. Dataset name: `Gold Document Embeddings`
3. 아래 SQL 입력 후 **Save**:
   ```sql
   SELECT * FROM `dev_haesung`.`gold`.`gold_document_embeddings`
   ```

### 3. 위젯 추가

대시보드 캔버스에서 **Add widget** 버튼으로 아래 위젯을 추가합니다.

#### 3-1. Counter 위젯 3개 (1행에 나란히 배치)

| 위젯명 | 데이터셋 | Measure |
|---|---|---|
| 총 문서 수 | Gold Document Summary | COUNT DISTINCT `document_id` |
| 총 페이지 수 | Gold Category Metrics | SUM `total_pages` |
| 총 글자 수 | Gold Category Metrics | SUM `total_chars` |

각 Counter 위젯 설정 방법:
1. 위젯 유형 **Counter** 선택
2. 해당 데이터셋 선택
3. **Value** 항목에서 컬럼과 집계 함수 선택

#### 3-2. Table 위젯 — 문서별 요약 정보

1. 위젯 유형 **Table** 선택
2. 데이터셋: `Gold Document Summary`
3. 표시할 컬럼 선택:
   - `document_id`
   - `source_file_name`
   - `page_count`
   - `total_chars`
   - `file_date`

#### 3-3. Bar Chart 위젯 — 문서별 페이지 수

1. 위젯 유형 **Bar** 선택
2. 데이터셋: `Gold Document Summary`
3. X축: `source_file_name`
4. Y축: `page_count` (SUM)

#### 3-4. Table 위젯 — AI 요약 결과

1. 위젯 유형 **Table** 선택
2. 데이터셋: `Gold Document AI Summary`
3. 표시할 컬럼 선택:
   - `document_id`
   - `page_count`
   - `ai_summary`

### 4. 대시보드 게시 (선택)

다른 사용자와 공유하려면:

1. 우상단 **Publish** 클릭
2. 권한 선택:
   - **Run as owner**: 대시보드 소유자 권한으로 실행 (공유 편리)
   - **Run as viewer**: 각 조회자 권한으로 실행 (보안 강화)
3. **Publish** 클릭 후 생성된 URL 공유

---

## 실행 방법

1. S3 Landing Zone에 PDF 파일 업로드
   ```
   s3://databricks-storage-7474657118263619/unity-catalog/7474657118263619/landing/documents/
   ```
2. 파이프라인 **Start** 클릭 (incremental update)
3. 스키마 변경 시 **Full Refresh** 선택 후 실행
4. 파이프라인 완료 후 대시보드에서 결과 확인

---

## 자동 트리거

이 파이프라인은 Lakeflow Job의 **File Arrival 트리거**로 S3에 파일이 도착하면 자동 실행됩니다.

| 항목 | 설정값 |
|---|---|
| Job 이름 | `databricks-pipeline-sample-batch` |
| Job ID | `504229728474154` |
| 트리거 방식 | File Arrival |
| 감시 경로 | `s3://databricks-storage-7474657118263619/unity-catalog/7474657118263619/landing/documents/` |
| 최소 실행 간격 | 600초 (10분) |
| 마지막 변경 후 대기 | 300초 (5분) |

S3 Landing Zone에 새 PDF가 적재되면, 마지막 파일 변경 후 5분 대기 → 파이프라인 자동 실행 → Staging → Bronze → Silver → Gold 전체 레이어를 처리합니다.

> **참고**: 트리거 설정을 변경하려면 Lakeflow Jobs 콘솔에서 해당 Job의 Trigger를 수정하세요.

---

## 사용 AI 함수

| 함수 | 위치 | 모델/버전 | 설명 |
|---|---|---|---|
| `ai_parse_document()` | silver_documents.py | v2.0 | PDF 바이너리에서 구조화된 텍스트·표·그림 요소 추출 |
| `ai_query()` | gold_document_ai_summary.py | `databricks-meta-llama-3-3-70b-instruct` | 문서별 한국어 3\~5문장 요약 생성 (figure 설명 포함) |
| `ai_query()` | gold_document_ai_summary.py | `databricks-meta-llama-3-3-70b-instruct` | 핵심 키워드 10\~20개 추출 (JSON 배열) |
| `ai_query()` | gold_document_ai_summary.py | `databricks-meta-llama-3-3-70b-instruct` | 문서 메타데이터 추출 (doc_category, insurance_product_type 등 JSON) |
| — | silver_document_chunks.py | — | 요소별 오버랩 청킹 (AI 미사용, 고정 길이 슬라이딩 윈도우 알고리즘) |
| — | gold_document_embeddings.py | `databricks-qwen3-embedding-0-6b` | Vector Search가 chunk_content에서 자동 임베딩 |

> **참고**: `gold_document_ai_summary.py`에는 OpenAI API 기반 요약 코드가 주석 처리되어 있으며, API 키 발급 후 활성화할 수 있습니다.

---

## Vector Search 연동

`gold_document_embeddings` 테이블을 생성한 뒤, 아래 단계로 Vector Search 인덱스를 설정합니다.

### 1. Vector Search 엔드포인트 생성

```python
from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()
vsc.create_endpoint(name="document-search-endpoint")
```

### 2. Delta Sync 인덱스 생성

```python
vsc.create_delta_sync_index(
    endpoint_name="document-search-endpoint",
    index_name="dev_haesung.gold.gold_document_embeddings_index",
    source_table_name="dev_haesung.gold.gold_document_embeddings",
    pipeline_type="TRIGGERED",
    primary_key="chunk_id",
    embedding_source_columns=[{
        "name": "chunk_content",
        "model_endpoint_name": "databricks-qwen3-embedding-0-6b"
    }],
)
```

### 3. 유사도 검색 (RAG)

```python
results = vsc.get_index(
    endpoint_name="document-search-endpoint",
    index_name="dev_haesung.gold.gold_document_embeddings_index",
).similarity_search(
    query_text="보험 보장 내용",
    columns=["chunk_content", "document_id", "element_type"],
    num_results=5,
)
```

> **참고**: 인덱스 동기화는 소스 테이블(`dev_haesung.gold.gold_document_embeddings`) 업데이트 시 자동 또는 수동(triggered)으로 실행됩니다.

---

## 설정값 관리 (`config.py`)

모든 하드코딩 값은 `config.py`에서 중앙 관리됩니다. 코드 수정 없이 설정만 변경하려면 이 파일을 수정하세요.

| 설정값 | 기본값 | 설명 |
|---|---|---|
| `S3_LANDING_PATH_DEFAULT` | S3 경로 | Bronze 수집 기본 경로 |
| `AI_PARSE_DOCUMENT_VERSION` | `"2.0"` | ai_parse_document 버전 |
| `TEXT_PREVIEW_LENGTH` | `500` | 텍스트 미리보기 길이 |
| `LLM_MODEL_NAME` | `databricks-meta-llama-3-3-70b-instruct` | AI 요약·청킹용 LLM |
| `AI_INPUT_MAX_CHARS` | `3000` | AI 입력 최대 글자수 |
| `AI_MAX_TOKENS` | `300` | AI 출력 최대 토큰 |
| `CHUNK_SIZE` | `500` | 청크 크기 (글자수) |
| `CHUNK_OVERLAP` | `100` | 청크 간 중복 |
| `EMBEDDING_MODEL_ENDPOINT` | `databricks-qwen3-embedding-0-6b` | 임베딩 모델 |
| `VS_ENDPOINT_NAME` | `document-search-endpoint` | Vector Search 엔드포인트 |
