# databricks-pipeline-sample

S3 Landing Zone에 카테고리별 폴더(예: `보험/CRM/`, `보험/상품/`)로 적재된 보험 문서(MD)를 Auto Loader로 수집하고, 텍스트를 정제·청킹한 뒤 외부 bge-m3 임베딩 서비스로 벡터를 계산하여 카테고리별 Vector Search 인덱스를 구성하는 Lakeflow Spark Declarative Pipeline입니다. `config.py`가 S3 Landing Zone의 1뎁스 폴더(카테고리)를 스캔해 Bronze/Silver/Gold 테이블을 카테고리마다 동적으로 생성합니다.

---

## 목차

1. [아키텍처](#아키텍처)
2. [데이터셋](#데이터셋)
3. [파일 구조](#파일-구조)
4. [파이프라인 배포 가이드](#파이프라인-배포-가이드)
5. [대시보드 콘솔 설정 가이드](#대시보드-콘솔-설정-가이드)
6. [실행 방법](#실행-방법)
7. [자동 트리거](#자동-트리거)
8. [사용 AI 함수 / 외부 서비스](#사용-ai-함수--외부-서비스)
9. [Vector Search 연동](#vector-search-연동)
10. [설정값 관리](#설정값-관리-configpy)

---

## 아키텍처

```
S3 Landing Zone (MD, 카테고리별 1뎁스 폴더)
        │
        ▼ Auto Loader (binaryFile, allowOverwrites=true, recursiveFileLookup=true)
┌───────────────────────────────┐
│   staging_documents            │  Streaming Table — 전 카테고리 공통
│   (staging 스키마)              │  파일 메타데이터 및 버전 이력 (append-only 이벤트 원장)
└───────────────────────────────┘
        │
        ├───────────────────────────────────────────────────────────┐
        │                                                           ▼ source_file별 집계 (row_number)
        │                                             ┌──────────────────────────────────┐
        │                                             │  staging_document_versions       │
        │                                             │  Materialized View — 버전 번호,   │
        │                                             │  최신 버전 여부 (staging 스키마)   │
        │                                             └──────────────────────────────────┘
        ▼ config.get_category_list()로 스캔한 카테고리마다 반복
        ▼ (source_file.startswith(category_path) 필터 + stream-static join)
┌───────────────────────────────┐
│  {category}_bronze_documents   │  Streaming Table — MD 바이너리 + 메타데이터 (bronze 스키마)
└───────────────────────────────┘
        │
        ▼ content.cast("STRING")  (ai_parse_document() 미사용 - 주석 처리로 보존)
┌───────────────────────────────┐
│  {category}_silver_documents   │  Streaming Table — MD 원문 텍스트 (full_text) (silver 스키마)
└───────────────────────────────┘
        │
        ▼ overlap_chunk UDF (문서 전체를 단일 요소로 오버랩 청킹)
┌──────────────────────────────────┐
│  {category}_silver_document_chunks │  Streaming Table (silver 스키마)
└──────────────────────────────────┘
        │
        ▼ chunk_id 할당 + bge-m3 FastAPI 임베딩 호출(embedding) + doc_path/agent_id 메타데이터
┌──────────────────────────────────┐
│  {category}_gold_document_embeddings │  Streaming Table — Vector Search 소스 (CDF 활성화, gold 스키마)
└──────────────────────────────────┘
        │
        ▼ Delta Sync
┌──────────────────────────────────┐
│  {category}_gold_document_embeddings_index │  Vector Index (카테고리별)
└──────────────────────────────────┘
```

> **카테고리별 동적 생성**: `bronze_documents.py`/`silver_documents.py`/`silver_document_chunks.py`/`gold_document_embeddings.py`는 각각 파일 최하단에서 `for _category in config.get_category_list(): ...`로 카테고리 수만큼 반복해 테이블 세트를 생성합니다. `staging_documents`/`staging_document_versions`만 전 카테고리 공통 단일 테이블입니다.

---

## 데이터셋

| 테이블 | 스키마 | 타입 | 설명 |
|---|---|---|---|
| `staging_documents` | `staging` | Streaming Table | 전 카테고리 공통. S3 파일 메타데이터 및 버전 이력 (바이너리 미저장, append-only 이벤트 원장) |
| `staging_document_versions` | `staging` | Materialized View | `staging_documents`를 source_file별로 집계한 버전 번호(`version_number`)/최신 버전 여부(`is_latest_version`) |
| `{category}_bronze_documents` | `bronze` | Streaming Table (카테고리별 동적 생성) | S3 MD 원본 바이너리 + staging 메타데이터 |
| `{category}_silver_documents` | `silver` | Streaming Table (카테고리별 동적 생성) | MD `content`를 텍스트로 캐스팅한 원문 (`ai_parse_document()` 미사용) |
| `{category}_silver_document_chunks` | `silver` | Streaming Table (카테고리별 동적 생성) | 문서 전체를 `overlap_chunk` UDF로 오버랩 청킹 (RAG 벡터검색용) |
| `{category}_gold_document_embeddings` | `gold` | Streaming Table (카테고리별 동적 생성) | Vector Search 소스 테이블. bge-m3 FastAPI 서비스로 계산한 `embedding` 벡터, `doc_path`/`agent_id`를 담은 `metadata` 컬럼 포함 (CDF 활성화) |
| `{category}_gold_document_embeddings_index` | `gold` (Vector Index) | Foreign Table | `{category}_gold_document_embeddings.embedding`을 Delta Sync로 색인한 카테고리별 벡터 인덱스 |

> **카테고리 스캔·네이밍**: `config.get_category_list()`가 파이프라인 그래프 빌드 시점에 S3 Landing Zone(`보험/` 바로 아래 1뎁스 폴더, 구조는 [diagram/s3_structure.md](diagram/s3_structure.md) 참고)을 스캔해 카테고리 목록을 얻습니다. 테이블명은 `config.get_table_name()`(`CATEGORY_NAME_MAP`)으로 한글 카테고리명을 영문명으로 변환해 사용합니다 (예: `CRM` → `crm_bronze_documents`, `발급` → `issuance_bronze_documents`).

> **S3 버전 관리**: S3 버킷 버저닝은 콘솔에서 활성화되어 있습니다. `staging_document_versions`는 파일 재업로드 시마다 메타데이터(도착 순서/최신 여부)만 추적하며, 과거 버전의 실제 파일 콘텐츠를 S3 VersionId와 연동해 조회/대조하는 기능은 이번 범위에서 제외했습니다. **TODO**: 필요 시 별도 작업으로 진행.

### document_id 생성 로직

`document_id`는 `bronze_documents.py`에서 S3 바이너리와 join하는 시점에 생성되며, 이후 silver/gold 전 레이어에 그대로 전파됩니다.

- **생성 규칙**: `source_file_name`(파일명, 경로 제외)에서 마지막 확장자만 정규식(`\.[^.]+$`)으로 제거
  ```python
  F.regexp_replace(F.col("source_file_name"), r"\.[^.]+$", "")
  ```
- **예시**: `agreement_v1.md` → `document_id = "agreement_v1"`
- **입력 컬럼**: `source_file_name`은 `staging_document.py`에서 S3 전체 경로(`path`)의 마지막 세그먼트만 추출한 값 (`F.element_at(F.split(F.col("path"), "/"), -1)`)

> **주의(경로 미포함)**: 폴더 경로는 `document_id`에 반영되지 않으므로, 같은 카테고리 폴더 내 서로 다른 하위 폴더에 동일한 파일명이 존재하면 `document_id`가 충돌합니다. 다른 카테고리끼리는 테이블 자체가 분리되어 있어 충돌하지 않습니다.
>
> **주의(재업로드 시 재사용)**: 파일이 같은 이름으로 재업로드(버전 갱신)되면 `document_id`는 이전과 동일하게 생성됩니다. `staging_document_versions`는 버전 이력(`version_number`/`is_latest_version`)을 별도로 추적하지만, `{category}_bronze_documents`는 `document_id`를 PRIMARY KEY(`` pk_{table_name}_bronze_documents ``)로 선언하고 있어 재업로드 시 동일 `document_id` 레코드가 갱신/충돌될 수 있습니다.

---

## 파일 구조

```
databricks-pipeline-sample/
├── README.md
├── config.py                                # 파이프라인 설정값 + 카테고리 스캔/매핑 로직 중앙 관리
├── databricks.yml                           # Databricks Asset Bundle (DAB) 정의 - 파이프라인/Job 배포
├── compute-ai-search-setup-guide.md         # 컴퓨트 & Vector Search 콘솔 설정 가이드
├── dev_haesung_pipeline_architecture.md     # 상세 아키텍처/스키마 문서 (개인 dev 카탈로그 기준 예시)
├── dev_haesung_default_ERD.py               # matplotlib 기반 ERD 이미지 생성 노트북
├── file-arrival-trigger-troubleshooting.md  # File Arrival 트리거 트러블슈팅 가이드
├── setup_vector_search_index.py             # [Job에서 실행] 카테고리별 Vector Search 인덱스 생성/동기화 노트북
├── copy_gold_and_sync_indexes.py            # 카테고리별 Gold 테이블을 통합 Delta Table로 합쳐 단일 인덱스로 동기화하는 노트북 (아직 Job에 미연결)
├── vector_search_index.py                   # 카테고리별 인덱스 동기화만 수행하는 경량 노트북
├── diagram/                                 # 아키텍처 다이어그램 (Mermaid, S3 구조)
├── staging/
│   ├── staging_document.py                  # [사용] S3 파일 도착 이벤트 원장 (staging.staging_documents)
│   ├── staging_document_versions.py         # [사용] source_file별 버전 이력 집계 (staging.staging_document_versions)
│   └── staging_documents.py                 # [미사용] 레거시 중복 파일 - databricks.yml에서 참조되지 않음
├── bronze/
│   └── bronze_documents.py                  # staging + S3 binary join, 카테고리별 동적 생성 (bronze.{table}_bronze_documents)
├── silver/
│   ├── silver_documents.py                  # 텍스트 디코딩, 카테고리별 동적 생성 (silver.{table}_silver_documents)
│   └── silver_document_chunks.py            # 오버랩 청킹, 카테고리별 동적 생성 (silver.{table}_silver_document_chunks)
└── gold/
    └── gold_document_embeddings.py          # bge-m3 임베딩 + 메타데이터, 카테고리별 동적 생성 (gold.{table}_gold_document_embeddings)
```

> **staging 폴더 관련 참고**: `databricks.yml`은 `staging_document.py`(단수)와 `staging_document_versions.py`만 파이프라인 라이브러리로 등록합니다. `staging_documents.py`(복수)는 이전 버전의 잔재 파일로, 그래프에 포함되지 않습니다.

---

## 파이프라인 배포 가이드

### 1. Databricks Asset Bundle (DAB)로 배포하기

저장소 루트의 `databricks.yml`이 파이프라인과 Job을 함께 정의합니다.

```bash
databricks bundle deploy -t dev
databricks bundle run rag_pipeline_job -t dev
```

| 리소스 | 내용 |
|---|---|
| 파이프라인 | `rag_document_processing` — `staging/bronze/silver/gold`의 모든 `.py`를 소스로 등록 |
| Job | `rag_pipeline_job` — ① `run_pipeline` 태스크로 파이프라인 실행 → ② 완료 후 `setup_vector_search_index.py` 노트북 태스크 실행 |
| 변수 | `catalog`(기본 `a_ws_ard_dev_ane2_01`), `schema`(기본 `default`), `s3_landing_path`(기본 `s3://a-s3-dbx-dev-ane2-aegis01/보험/`) |
| 타겟 | `dev`(기본), `prod`(카탈로그/스키마는 배포 전 TODO 확인 필요) |

> **참고**: 각 `.py` 파일이 `staging.staging_documents`, `bronze.{table}_bronze_documents`처럼 스키마를 명시한 fully-qualified 이름을 사용하므로, 실제 테이블은 `{catalog}.staging`, `{catalog}.bronze`, `{catalog}.silver`, `{catalog}.gold`에 생성되고 `databricks.yml`의 `schema: default` 값은 무시됩니다.
>
> ⚠ **인덱스 생성 방식 확인 필요**: Job에 연결된 `setup_vector_search_index.py`는 아직 `embedding_source_columns`(Vector Search가 `chunk_content`에서 `databricks-qwen3-embedding-0-6b`로 자동 계산) 방식을 사용합니다. 반면 파이프라인(`gold_document_embeddings.py`)은 이미 bge-m3 API로 직접 계산한 `embedding` 컬럼을 저장하도록 전환되어 있습니다. 새로 추가된 `copy_gold_and_sync_indexes.py`(카테고리 통합 테이블 + `embedding_vector_column` 방식)가 현재 파이프라인의 임베딩 방식과 일치하지만 아직 Job에는 연결되지 않았습니다 — 인덱스를 만들기 전에 두 스크립트 중 어떤 방식을 쓸지 먼저 정리하세요 (자세한 내용은 [Vector Search 연동](#vector-search-연동) 참고).

### 2. 콘솔에서 수동으로 생성하기 (대안)

DAB 대신 콘솔에서 직접 파이프라인을 만드는 경우의 절차입니다.

#### 2-1. 파이프라인 생성

1. 왼쪽 메뉴에서 **Lakeflow** → **Pipelines** 클릭
2. 우상단 **Create pipeline** 버튼 클릭
3. Pipeline name 입력: `databricks-pipeline-sample`

#### 2-2. 소스 파일 추가

1. **Source code** 섹션에서 **Add source code** 클릭
2. 파일 탐색기에서 아래 경로의 폴더 선택 (glob 패턴 자동 적용)
   ```
   /Users/{username}/databricks-pipeline-sample/
   ```
3. 저장 시 glob 패턴이 아래와 같이 설정됨:
   ```
   /Users/{username}/databricks-pipeline-sample/**
   ```

#### 2-3. Catalog / Schema 설정

1. **Destination** 섹션에서 **Storage options** 클릭
2. **Catalog** 항목에 사용할 카탈로그명 입력 (개인 dev 카탈로그 예시: `dev_haesung`, DAB 기본값은 `a_ws_ard_dev_ane2_01`)
3. **Target schema** 항목에 `default` 입력 (각 테이블이 fully-qualified name으로 스키마를 지정하므로 기본값은 무시됨)

> **참고**: 실제 테이블은 각각 `{catalog}.staging`, `{catalog}.bronze`, `{catalog}.silver`, `{catalog}.gold` 스키마에 발행됩니다.

#### 2-4. Compute 설정

| 항목 | 설정값 |
|---|---|
| Compute type | Serverless |
| Photon acceleration | 활성화 (체크) |
| Channel | Current |

#### 2-5. Configuration 추가 (S3 경로)

1. **Advanced** 섹션 → **Configuration** 클릭
2. **Add configuration** 버튼 클릭
3. 아래 키-값 입력:

| Key | Value |
|---|---|
| `s3_landing_path` | `s3://a-s3-dbx-dev-ane2-aegis01/보험/` |

> **참고**: 이 값은 `staging_document.py`/`bronze_documents.py`에서 `spark.conf.get("s3_landing_path", ...)` 으로 참조됩니다. S3 경로 변경 시 코드 수정 없이 이 값만 바꾸면 됩니다. 이 경로 바로 아래 1뎁스 폴더 구조가 카테고리 목록이 되므로, 새 카테고리 폴더를 추가하면 다음 Full Refresh 시 해당 카테고리의 bronze/silver/gold 테이블이 자동 생성됩니다.

#### 2-6. 파이프라인 저장 및 실행

1. 우상단 **Save** 클릭 후 **Start** 클릭 (일반 업데이트)
2. 스키마 변경 시: **Start** 옆 화살표(▼) → **Full refresh** 선택

> 컴퓨트/AI 검색 콘솔 설정의 더 자세한 절차는 [compute-ai-search-setup-guide.md](compute-ai-search-setup-guide.md)를 참고하세요.

---

## 대시보드 콘솔 설정 가이드

콘솔에서 수동으로 AI/BI 대시보드를 생성·설정하는 방법입니다. 카테고리마다 테이블이 분리되어 있으므로, 아래 예시는 카테고리 하나(`{category}`, 예: `crm`)를 대상으로 합니다.

### 1. 대시보드 생성

1. 왼쪽 메뉴에서 **SQL** → **Dashboards** 클릭
2. 우상단 **Create dashboard** 버튼 클릭
3. 대시보드 이름 입력: `보험 문서 파이프라인 결과 대시보드`

### 2. 데이터셋 추가

대시보드 편집 화면 하단의 **Data** 탭에서 각 데이터셋을 추가합니다.

**데이터셋 1 — {category} Silver Document Chunks**

1. **Create dataset** 클릭
2. Dataset name: `{category} Silver Document Chunks`
3. 아래 SQL 입력 후 **Save**:
   ```sql
   SELECT * FROM `{catalog}`.`silver`.`{category}_silver_document_chunks`
   ```

**데이터셋 2 — {category} Gold Document Embeddings**

1. **Create dataset** 클릭
2. Dataset name: `{category} Gold Document Embeddings`
3. 아래 SQL 입력 후 **Save**:
   ```sql
   SELECT * FROM `{catalog}`.`gold`.`{category}_gold_document_embeddings`
   ```

> **여러 카테고리를 한 화면에 보고 싶다면**: `copy_gold_and_sync_indexes.py`가 만드는 통합 테이블(`` `{catalog}`.`gold`.`gold_embeddings` ``, 모든 카테고리를 UNION ALL로 합친 테이블)을 데이터셋으로 사용하거나, 카테고리별 SQL을 직접 `UNION ALL`로 이어 붙이세요. 카테고리 구분은 `metadata` 컬럼의 `doc_path`/`agent_id`로 필터링할 수 있습니다.

### 3. 위젯 추가

대시보드 캔버스에서 **Add widget** 버튼으로 아래 위젯을 추가합니다 (실제 테이블 컬럼 기준).

#### 3-1. Counter 위젯 3개 (1행에 나란히 배치)

| 위젯명 | 데이터셋 | Measure |
|---|---|---|
| 총 문서 수 | {category} Silver Document Chunks | COUNT DISTINCT `document_id` |
| 총 청크 수 | {category} Gold Document Embeddings | COUNT `chunk_id` |
| 총 원본 파일 수 | {category} Silver Document Chunks | COUNT DISTINCT `source_file_name` |

각 Counter 위젯 설정 방법:
1. 위젯 유형 **Counter** 선택
2. 해당 데이터셋 선택
3. **Value** 항목에서 컬럼과 집계 함수 선택

#### 3-2. Table 위젯 — 청크별 상세 정보

1. 위젯 유형 **Table** 선택
2. 데이터셋: `{category} Gold Document Embeddings`
3. 표시할 컬럼 선택:
   - `document_id`
   - `source_file_name`
   - `chunk_idx`
   - `chunk_type`
   - `chunked_at`

#### 3-3. Bar Chart 위젯 — 문서별 청크 수

1. 위젯 유형 **Bar** 선택
2. 데이터셋: `{category} Gold Document Embeddings`
3. X축: `document_id`
4. Y축: `chunk_id` (COUNT)

### 4. 대시보드 게시 (선택)

다른 사용자와 공유하려면:

1. 우상단 **Publish** 클릭
2. 권한 선택:
   - **Run as owner**: 대시보드 소유자 권한으로 실행 (공유 편리)
   - **Run as viewer**: 각 조회자 권한으로 실행 (보안 강화)
3. **Publish** 클릭 후 생성된 URL 공유

---

## 실행 방법

1. S3 Landing Zone에 카테고리 폴더 구조([diagram/s3_structure.md](diagram/s3_structure.md) 참고)로 MD 파일 업로드
   ```
   s3://a-s3-dbx-dev-ane2-aegis01/보험/{카테고리명}/...
   ```
2. 파이프라인 **Start** 클릭 (incremental update) — `config.get_category_list()`가 S3 폴더를 다시 스캔해 새 카테고리가 있으면 해당 테이블 세트를 추가로 생성
3. 스키마 변경 시 **Full Refresh** 선택 후 실행
4. 파이프라인 완료 후 Job의 두 번째 태스크(`setup_vector_search_index.py`)가 카테고리별 Vector Search 인덱스를 동기화
5. 대시보드에서 결과 확인

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

S3 Landing Zone에 새 MD 파일이 적재되면, 마지막 파일 변경 후 5분 대기 → 파이프라인 자동 실행 → 그래프 빌드 시점에 카테고리 목록을 다시 스캔 → 감지된 모든 카테고리에 대해 Staging → Bronze → Silver → Gold 전체 레이어를 처리합니다.

> **참고**: 트리거 설정을 변경하려면 Lakeflow Jobs 콘솔에서 해당 Job의 Trigger를 수정하세요. File Arrival 트리거 구성 중 겪은 이슈(SNS/SQS 권한 등)는 [file-arrival-trigger-troubleshooting.md](file-arrival-trigger-troubleshooting.md)를 참고하세요.

---

## 사용 AI 함수 / 외부 서비스

| 함수/서비스 | 위치 | 모델/버전 | 상태 |
|---|---|---|---|
| `ai_parse_document()` | silver_documents.py | v2.0 | **비활성** — MD 전환으로 주석 처리됨. PDF 바이너리에서 구조화된 텍스트·표·그림 요소 추출 — PDF 복귀 시 재활성화 |
| `ai_query()` | (사용 안 함) | `databricks-meta-llama-3-3-70b-instruct` | **미사용** — 이전 시멘틱 청킹 방식(`config.CHUNKING_*`)의 잔재 설정만 남아 있으며 현재 코드에서 호출되지 않음 |
| `overlap_chunk` (Python UDF) | silver_document_chunks.py | — | **활성** — AI가 아닌 슬라이딩 윈도우 방식으로 오버랩 청킹 (`CHUNK_SIZE`/`CHUNK_OVERLAP`) |
| bge-m3 임베딩 API | gold_document_embeddings.py (`embed_with_bge_m3_api`) | `bge-m3` (외부 FastAPI 서비스, `config.EMBEDDING_API_*`) | **활성** — `chunk_content`를 배치로 전송해 1024차원 `embedding` 컬럼을 계산·저장 |
| doc_path/agent_id 매핑 | gold_document_embeddings.py (`_resolve_agent_id`) | — | **활성** — `source_file` 경로를 `doc_path`로 변환하고 `config.DOC_PATH_AGENT_MAP` 규칙으로 `agent_id`를 판정해 `metadata` JSON에 저장 |

> **참고**: 청킹(`silver_document_chunks.py`)은 AI 함수가 아닌 `overlap_chunk` Python UDF(슬라이딩 윈도우 방식)로 수행되며, Silver 단계까지는 AI 함수를 전혀 사용하지 않습니다. 임베딩 역시 Databricks 내장 AI 함수가 아니라 다른 팀이 운영하는 외부 bge-m3 FastAPI 서비스를 HTTP로 직접 호출해 계산합니다.

---

## Vector Search 연동

`{category}_gold_document_embeddings` 테이블을 생성한 뒤, 아래 단계로 Vector Search 인덱스를 설정합니다. 인덱스는 이 파이프라인의 `.py` 코드가 아니라 별도 노트북(`setup_vector_search_index.py` 또는 `copy_gold_and_sync_indexes.py`)에서 생성합니다.

> ⚠ **두 가지 방식이 공존 중입니다**: `databricks.yml` Job에 연결된 `setup_vector_search_index.py`는 아직 (A) `embedding_source_columns` 방식(Vector Search가 `chunk_content`에서 자동 계산)을 사용합니다. 하지만 파이프라인은 이미 bge-m3 API로 `embedding` 컬럼을 직접 계산해 저장하도록 전환되어 있으므로, 실제로는 (B) `embedding_vector_column` 방식(파이프라인이 저장한 벡터를 그대로 색인)이 현재 스키마와 일치합니다. 새 카테고리 통합 노트북 `copy_gold_and_sync_indexes.py`는 이미 (B) 방식 + 카테고리 통합 테이블로 작성되어 있지만 Job에는 아직 연결되지 않았습니다. 인덱스를 새로 만들 때는 이 두 스크립트 중 무엇을 실제로 실행할지 먼저 확인하세요.

### 1. Vector Search 엔드포인트 생성

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType

w = WorkspaceClient()
w.vector_search_endpoints.create_endpoint(
    name="document-search-endpoint",  # config.VS_ENDPOINT_NAME
    endpoint_type=EndpointType.STANDARD,
)
```

### 2-A. (현재 Job 연결) 카테고리별 Delta Sync 인덱스 — 자동 임베딩 방식

`setup_vector_search_index.py`가 카테고리마다 아래와 같은 인덱스를 생성합니다.

```python
from databricks.sdk.service.vectorsearch import DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType

for category in config.get_category_list():
    table_name = config.get_table_name(category)
    w.vector_search_indexes.create_index(
        name=f"{CATALOG}.gold.{table_name}_gold_document_embeddings_index",
        endpoint_name="document-search-endpoint",
        primary_key="chunk_id",
        index_type="DELTA_SYNC",
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=f"{CATALOG}.gold.{table_name}_gold_document_embeddings",
            pipeline_type=PipelineType("TRIGGERED"),
            embedding_source_columns=[
                EmbeddingSourceColumn(name="chunk_content", embedding_model_endpoint_name="databricks-qwen3-embedding-0-6b"),
            ],
        ),
    )
```

### 2-B. (파이프라인 스키마와 일치) `embedding_vector_column` 방식 — bge-m3 사전 계산 벡터 사용

카테고리별로 만들려면 `EmbeddingSourceColumn`을 `EmbeddingVectorColumn`으로 바꿔 파이프라인이 이미 계산한 `embedding` 컬럼을 그대로 색인합니다. `copy_gold_and_sync_indexes.py`는 이 방식으로 카테고리를 통합한 단일 테이블(`` `{catalog}`.`gold`.`gold_embeddings` ``)과 단일 인덱스(`` `{catalog}`.`gold`.`gold_embeddings_index` ``)를 만듭니다.

```python
from databricks.sdk.service.vectorsearch import DeltaSyncVectorIndexSpecRequest, EmbeddingVectorColumn, PipelineType

w.vector_search_indexes.create_index(
    name=f"{CATALOG}.gold.gold_embeddings_index",
    endpoint_name="document-search-endpoint",
    primary_key="chunk_id",
    index_type="DELTA_SYNC",
    delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
        source_table=f"{CATALOG}.gold.gold_embeddings",
        pipeline_type=PipelineType("TRIGGERED"),
        embedding_vector_columns=[
            EmbeddingVectorColumn(name="embedding", embedding_dimension=1024),  # config.EMBEDDING_API_DIMENSION
        ],
    ),
)
```

### 3. 유사도 검색 (RAG)

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
results = vsc.get_index(
    endpoint_name="document-search-endpoint",
    index_name=f"{CATALOG}.gold.{table_name}_gold_document_embeddings_index",
).similarity_search(
    query_text="보험 보장 내용",
    columns=["chunk_id", "chunk_content", "document_id", "source_file_name", "metadata"],
    num_results=5,
)
```

> **참고**: 인덱스 동기화는 소스 테이블 업데이트 시 자동 또는 수동(`sync_index`, Triggered 모드)으로 실행됩니다. 콘솔 기반 절차는 [compute-ai-search-setup-guide.md](compute-ai-search-setup-guide.md)를 참고하세요.

---

## 설정값 관리 (`config.py`)

모든 하드코딩 값은 `config.py`에서 중앙 관리됩니다. 코드 수정 없이 설정만 변경하려면 이 파일을 수정하세요.

### 사용 중인 설정값

| 설정값 | 기본값 | 설명 |
|---|---|---|
| `S3_LANDING_PATH_DEFAULT` | `s3://a-s3-dbx-dev-ane2-aegis01/보험/` | Staging/Bronze 수집 기본 경로 |
| `get_category_list()` | — | S3 Landing Zone 1뎁스 폴더명을 카테고리 목록으로 스캔 (그래프 빌드 시점 호출) |
| `CATEGORY_NAME_MAP` / `get_table_name()` | 11개 카테고리 매핑 | 한글 카테고리명 → 테이블/인덱스명용 영문 slug 변환 (매핑에 없으면 소문자 변환) |
| `DOC_PATH_AGENT_MAP` / `get_agent_id()` | 4개 에이전트 매핑 | `doc_path`(source_file 경로를 `>`로 이은 문자열) 기준 `agent_id` 판정 규칙. `gold_document_embeddings.py`가 값을 복사해 UDF에서 사용 |
| `AI_PARSE_DOCUMENT_VERSION` | `"2.0"` | ai_parse_document 버전 (현재 비활성 - MD 전환으로 미사용, 주석 코드에만 참조됨) |
| `CHUNK_SIZE` | `1000` | 청크 크기 (글자수, `overlap_chunk` UDF) |
| `CHUNK_OVERLAP` | `200` | 청크 간 중복 (글자수) |
| `VS_ENDPOINT_NAME` | `document-search-endpoint` | Vector Search 엔드포인트명 |
| `EMBEDDING_SOURCE_COLUMN` | `chunk_content` | 임베딩 대상 컬럼명 |
| `EMBEDDING_API_URL` | `http://84.14.160.10:8000/embed` | bge-m3 임베딩 FastAPI 서비스 엔드포인트 (dev) |
| `EMBEDDING_API_KEY` | `ai_ready_data_embedding_dev` | 임베딩 API 인증 키 (`x-api-key` 헤더, dev 환경용) |
| `EMBEDDING_API_MODEL_NAME` | `bge-m3` | 임베딩 모델명 |
| `EMBEDDING_API_DIMENSION` | `1024` | bge-m3 임베딩 차원 (dense vector) |
| `EMBEDDING_API_BATCH_SIZE` | `32` | 임베딩 API 요청 1회당 텍스트 배치 크기 |
| `EMBEDDING_API_TIMEOUT` | `30` | 임베딩 API 타임아웃(초) |

### 정의만 되어 있고 현재 코드에서 참조되지 않는 값 (예약/레거시)

이전 시멘틱 청킹(`ai_query`)·경로 기반 청킹 전략·`databricks-qwen3-embedding-0-6b` 자동 임베딩 시절의 설정이 정리되지 않은 채 남아 있습니다. 코드에서 실제로 읽지 않으므로 값을 바꿔도 파이프라인 동작에는 영향이 없습니다.

| 설정값 | 기본값 |
|---|---|
| `TEXT_PREVIEW_LENGTH` | `500` |
| `CHUNKING_LLM_MODEL` / `CHUNKING_INPUT_MAX_CHARS` / `CHUNKING_MAX_TOKENS` / `CHUNKING_PROMPT` | `databricks-meta-llama-3-3-70b-instruct` 기반 시멘틱 청킹용 (미사용) |
| `PATH_CHUNKING_STRATEGIES` | 경로 glob 기반 청킹 전략 예시 (미사용) |
| `VS_INDEX_NAME` / `VS_SOURCE_TABLE` | `dev_haesung.gold.*` (카테고리 분리 이전의 단일 테이블 시절 이름) |
| `VS_NUM_RESULTS` | `5` |
| `RAG_LLM_MODEL` | `databricks-meta-llama-3-3-70b-instruct` |
