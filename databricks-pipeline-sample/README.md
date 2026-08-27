# databricks-pipeline-sample

S3에 적재된 보험 문서(MD)를 Auto Loader로 수집하고, 텍스트를 정제·청킹한 뒤 Vector Search로 임베딩하여 RAG(Retrieval-Augmented Generation) 파이프라인을 구성하는 Lakeflow Spark Declarative Pipeline입니다.

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
S3 Landing Zone (MD)
        │
        ▼ Auto Loader (binaryFile, allowOverwrites=true, recursiveFileLookup=true)
┌───────────────────────────────┐
│   staging_documents           │  Streaming Table — 파일 메타데이터 및 버전 이력
│   (dev_haesung.staging)       │  S3 버전 관리, 파일 도착 이력 추적 (append-only 이벤트 원장)
└───────────────────────────────┘
        │
        ├───────────────────────────────────────────────────────────┐
        │                                                           ▼ source_file별 집계 (row_number)
        │                                             ┌──────────────────────────────────┐
        │                                             │  staging_document_versions       │
        │                                             │  Materialized View — 버전 번호,   │
        │                                             │  최신 버전 여부 (dev_haesung.staging) │
        │                                             └──────────────────────────────────┘
        ▼ Stream-static join (staging metadata + S3 binary)
┌───────────────────────────────┐
│   bronze_documents            │  Streaming Table — MD 바이너리 + 메타데이터
│   (dev_haesung.bronze)        │
└───────────────────────────────┘
        │
        ▼ content.cast("STRING")  (ai_parse_document() 미사용 - 주석 처리로 보존)
┌───────────────────────────────┐
│   silver_documents            │  Streaming Table — MD 원문 텍스트 (full_text)
│   (dev_haesung.silver)        │
└───────────────────────────────┘
        │
        ▼
┌──────────────────────────────────┐
│   silver_document_chunks         │  Streaming Table — 요소별 오버랩 청킹
│ (overlap_chunk UDF - RAG용)      │
│ (dev_haesung.silver)             │
└──────────────────────────────────┘
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
| `staging_documents` | `dev_haesung.staging` | Streaming Table | S3 파일 메타데이터 및 버전 이력 (바이너리 미저장, append-only 이벤트 원장) |
| `staging_document_versions` | `dev_haesung.staging` | Materialized View | `staging_documents`를 source_file별로 집계한 버전 번호(`version_number`)/최신 버전 여부(`is_latest_version`) |
| `bronze_documents` | `dev_haesung.bronze` | Streaming Table | S3 MD 원본 바이너리 + staging 메타데이터 |
| `silver_documents` | `dev_haesung.silver` | Streaming Table | MD `content`를 텍스트로 캐스팅한 원문 (`ai_parse_document()` 미사용, `figure_descriptions`/`page_count`/`parsed_content`는 항상 NULL) |
| `silver_document_chunks` | `dev_haesung.silver` | Streaming Table | 문서 전체를 `overlap_chunk` UDF로 오버랩 청킹 (RAG 벡터검색용) |
| `gold_document_embeddings` | `dev_haesung.gold` | Streaming Table | Vector Search 소스 테이블 (CDF 활성화) |

> **S3 버전 관리**: S3 버킷 버저닝은 콘솔에서 활성화되어 있습니다. `staging_document_versions`는 파일 재업로드 시마다 메타데이터(도착 순서/최신 여부)만 추적하며, 과거 버전의 실제 파일 콘텐츠를 S3 VersionId와 연동해 조회/대조하는 기능은 이번 범위에서 제외했습니다. **TODO**: 필요 시 별도 작업으로 진행.

### document_id 생성 로직

`document_id`는 `bronze_documents.py`에서 S3 바이너리와 join하는 시점에 생성되며, 이후 silver/gold 전 레이어에 그대로 전파됩니다.

- **생성 규칙**: `source_file_name`(파일명, 경로 제외)에서 마지막 확장자만 정규식(`\.[^.]+$`)으로 제거
  ```python
  F.regexp_replace(F.col("source_file_name"), r"\.[^.]+$", "")
  ```
- **예시**: `agreement_v1.md` → `document_id = "agreement_v1"`
- **입력 컬럼**: `source_file_name`은 `staging_documents.py`에서 S3 전체 경로(`path`)의 마지막 세그먼트만 추출한 값 (`F.element_at(F.split(F.col("path"), "/"), -1)`)

> **주의(경로 미포함)**: 폴더 경로는 `document_id`에 반영되지 않으므로, S3 Landing Zone 내 서로 다른 폴더에 동일한 파일명이 존재하면 `document_id`가 충돌합니다.
>
> **주의(재업로드 시 재사용)**: 파일이 같은 이름으로 재업로드(버전 갱신)되면 `document_id`는 이전과 동일하게 생성됩니다. `staging_document_versions`는 버전 이력(`version_number`/`is_latest_version`)을 별도로 추적하지만, `bronze_documents`는 `document_id`를 PRIMARY KEY(`pk_bronze_documents`)로 선언하고 있어 재업로드 시 동일 `document_id` 레코드가 갱신/충돌될 수 있습니다.

---

## 파일 구조

```
databricks-pipeline-sample/
├── README.md
├── config.py                              # 파이프라인 설정값 중앙 관리 모듈
├── file-arrival-trigger-troubleshooting.md # File Arrival 트리거 트러블슈팅 가이드
└── pipeline/
    ├── staging/
    │   └── staging_documents.py           # S3 파일 메타데이터 및 버전 이력 (dev_haesung.staging)
    ├── bronze/
    │   └── bronze_documents.py            # staging + S3 binary join (dev_haesung.bronze)
    ├── silver/
    │   ├── silver_documents.py            # ai_parse_document()로 텍스트 추출 (dev_haesung.silver)
    │   └── silver_document_chunks.py      # 요소별 오버랩 청킹 (dev_haesung.silver)
    └── gold/
        └── gold_document_embeddings.py    # Vector Search 소스 (dev_haesung.gold)
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
| `s3_landing_path` | `s3://a-s3-dbx-dev-ane2-aegis01/보험/` |

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

**데이터셋 1 — Gold Document Chunks**

1. **Create dataset** 클릭
2. Dataset name: `Gold Document Chunks`
3. 아래 SQL 입력 후 **Save**:
   ```sql
   SELECT * FROM `dev_haesung`.`silver`.`silver_document_chunks`
   ```

**데이터셋 2 — Gold Document Embeddings**

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

### 4. 대시보드 게시 (선택)

다른 사용자와 공유하려면:

1. 우상단 **Publish** 클릭
2. 권한 선택:
   - **Run as owner**: 대시보드 소유자 권한으로 실행 (공유 편리)
   - **Run as viewer**: 각 조회자 권한으로 실행 (보안 강화)
3. **Publish** 클릭 후 생성된 URL 공유

---

## 실행 방법

1. S3 Landing Zone에 MD 파일 업로드
   ```
   s3://a-s3-dbx-dev-ane2-aegis01/보험/
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
| 감시 경로 | `s3://a-s3-dbx-dev-ane2-aegis01/보험/` |
| 최소 실행 간격 | 600초 (10분) |
| 마지막 변경 후 대기 | 300초 (5분) |

S3 Landing Zone에 새 MD 파일이 적재되면, 마지막 파일 변경 후 5분 대기 → 파이프라인 자동 실행 → Staging → Bronze → Silver → Gold 전체 레이어를 처리합니다.

> **참고**: 트리거 설정을 변경하려면 Lakeflow Jobs 콘솔에서 해당 Job의 Trigger를 수정하세요.

---

## 사용 AI 함수

| 함수 | 위치 | 모델/버전 | 설명 |
|---|---|---|---|
| `ai_parse_document()` | silver_documents.py | v2.0 | **(비활성)** MD 전환으로 주석 처리됨. PDF 바이너리에서 구조화된 텍스트·표·그림 요소 추출 — PDF 복귀 시 재활성화 |
| `ai_query()` | gold_document_chunks.py | `databricks-meta-llama-3-3-70b-instruct` | 텍스트 요소 시멘틱 청킹 (의미 단위 분할) |
| — | gold_document_embeddings.py | `databricks-qwen3-embedding-0-6b` | Vector Search가 chunk_content에서 자동 임베딩 (파이프라인에서 제거됨) |

> **참고**: 청킹(`silver_document_chunks.py`)은 AI 함수가 아닌 `overlap_chunk` Python UDF(슬라이딩 윈도우 방식)로 수행되며, Silver 단계까지는 AI 함수를 전혀 사용하지 않습니다.

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
| `AI_PARSE_DOCUMENT_VERSION` | `"2.0"` | ai_parse_document 버전 (현재 비활성 - MD 전환으로 미사용, PDF 복귀 시 사용) |
| `TEXT_PREVIEW_LENGTH` | `500` | 텍스트 미리보기 길이 |
| `CHUNKING_LLM_MODEL` | `databricks-meta-llama-3-3-70b-instruct` | 시멘틱 청킹용 LLM |
| `CHUNK_SIZE` | `500` | 청크 크기 (글자수) |
| `CHUNK_OVERLAP` | `100` | 청크 간 중복 |
| `EMBEDDING_MODEL_ENDPOINT` | `databricks-qwen3-embedding-0-6b` | 임베딩 모델 |
| `VS_ENDPOINT_NAME` | `document-search-endpoint` | Vector Search 엔드포인트 |
