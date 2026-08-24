# 보험 — 데이터 파이프라인 아키텍처

## Overview

- **Schema:** `보험`
- **Layers:** `staging`, `bronze`, `silver`, `gold`
- **테이블 네이밍**: `staging`은 전 상품 공통 단일 테이블(`staging_documents`)이고, `bronze`/`silver`/`gold`는 상품별로 동적 생성되는 `{상품명}_{원래 테이블명}` 구조입니다 (예: `종신보험_bronze_documents`, `종신보험_silver_documents`, `종신보험_gold_document_ai_summary`). `config.get_product_list()`가 파이프라인 그래프 빌드 시점에 S3 Landing Zone(`보험/` 바로 아래 1뎁스 폴더)을 스캔해 상품 목록을 얻고, 각 bronze/silver/gold 파이프라인 파일이 그 목록으로 for 루프를 돌며 상품마다 테이블을 생성합니다 — 자세한 내용은 [staging_documents 섹션](#1-staging_documents)의 참고 참조.
- **Pipeline:** `databricks-pipeline-sample` (보험 문서 RAG 파이프라인)
- **Pipeline ID:** `454e3aef-72c9-48da-80f7-4910933e8b4e`
- **Last updated:** 2026-08-24 (PDF → MD 소스 전환, staging_document_versions 추가, bronze/silver/gold 상품별 동적 테이블 생성 반영)

S3 Landing Zone(`s3://a-s3-dbx-dev-ane2-aegis01/보험/`)에 적재된 보험 문서(MD)를 Auto Loader로 수집하고, 텍스트를 정제·청킹한 뒤 Databricks AI 함수(`ai_query`)로 요약·임베딩하여 Vector Search 인덱스와 동기화, RAG(Retrieval-Augmented Generation)에 활용하는 Lakeflow Declarative Pipeline입니다.

> **소스 포맷 전환**: 파이프라인은 원래 PDF + `ai_parse_document()` 기반으로 설계되었으나, 현재는 MD 파일 업로드를 전제로 `ai_parse_document()` 호출 없이 바이너리 콘텐츠를 텍스트로 직접 디코딩합니다. 기존 PDF 파싱/요소별 청킹 로직은 삭제하지 않고 각 노트북에 주석 처리로 보존되어 있어 PDF로 복귀 시 재활성화할 수 있습니다.

---

## 파이프라인 시퀀스 흐름 (Mermaid)

File Arrival 트리거부터 Vector Search 동기화까지의 처리 순서입니다. bronze/silver/gold는 상품별로 동적 생성되므로, 아래 흐름은 상품 하나(`{product}`)에 대한 템플릿이며 실제로는 `config.get_product_list()`가 반환하는 상품 수만큼 병렬로 반복됩니다.

```mermaid
sequenceDiagram
    autonumber
    participant S3 as S3 Landing Zone
    participant Job as Lakeflow Job<br/>(File Arrival Trigger)
    participant Staging as staging_documents<br/>(전 상품 공통)
    participant Bronze as {product}_bronze_documents
    participant Silver as {product}_silver_documents
    participant LLM as Model Serving<br/>(ai_query)
    participant Chunks as {product}_silver_document_chunks
    participant Summary as {product}_gold_document_ai_summary
    participant Embeddings as {product}_gold_document_embeddings
    participant VS as Vector Search Index

    Note over S3: MD 업로드
    S3->>Job: 파일 도착 감지
    Note over Job: 마지막 파일 변경 후 300초 대기<br/>최소 실행 간격 600초
    Job->>Staging: 파이프라인 업데이트 시작
    Note over Job: 그래프 빌드 시점에 config.get_product_list()로<br/>S3 1뎁스 폴더를 스캔해 상품 목록 확보 →<br/>bronze/silver/gold 각 .py의 for 루프가 상품별 테이블 생성

    Staging->>S3: Auto Loader (cloudFiles, binaryFile,<br/>allowOverwrites=true, recursiveFileLookup=true)
    Staging->>Staging: 전 상품 파일 메타데이터 수집<br/>(append-only 이벤트 원장, 바이너리 미저장)
    Note over Staging: staging_documents 전체 read → source_file별<br/>row_number()로 version_number / is_latest_version 계산<br/>(내부적으로 staging_document_versions Materialized View로 구현)

    Staging->>Bronze: staging_documents 스트림 read<br/>+ source_file.startswith(product_path) 필터
    Bronze->>S3: binaryFile static read<br/>(product_path만 recursiveFileLookup=true)
    Bronze->>Bronze: stream-static join<br/>(source_file = path)<br/>document_id = source_file_name 확장자 제거

    Bronze->>Silver: {product}_bronze_documents 스트림 read
    Silver->>Silver: full_text = content.cast(STRING)<br/>(ai_parse_document 미사용 - PDF 로직은 주석 처리로 보존)<br/>figure_descriptions/page_count/parsed_content는 항상 NULL

    par Gold - AI 요약 / 키워드 / 메타데이터
        Silver->>Summary: {product}_silver_documents 스트림 read
        Summary->>LLM: ai_query() 요약 생성
        Summary->>LLM: ai_query() 키워드 추출
        Summary->>LLM: ai_query() 메타데이터(JSON) 추출
        LLM-->>Summary: summary, keywords, metadata
    and Silver/Gold - 청킹 및 임베딩 소스
        Silver->>Chunks: {product}_silver_documents 스트림 read
        Chunks->>Chunks: full_text 전체를 단일 요소로 오버랩 청킹<br/>(overlap_chunk UDF, AI 미사용 / CHUNK_SIZE=500, OVERLAP=100)
        Chunks->>Embeddings: {product}_silver_document_chunks 스트림 read
        Embeddings->>Embeddings: chunk_id = md5(document_id, element_idx, chunk_idx)
    end

    Embeddings->>VS: Delta Sync (CDF 기반, 상품별 인덱스)
    VS->>VS: chunk_content 자동 임베딩<br/>(databricks-qwen3-embedding-0-6b)

    Note over VS: RAG 애플리케이션에서<br/>similarity_search() 호출
```

---

## ERD (Mermaid)

```mermaid
erDiagram
    staging_documents {
        STRING source_file "S3 전체 경로 (자연키)"
        STRING source_file_name "원본 파일명"
        LONG file_size_bytes "파일 크기(바이트)"
        TIMESTAMP file_modified_at "파일 수정 시각"
        TIMESTAMP ingested_at "수집 시각"
        INT version_number "(버전 조회 시) source_file별 도착 순번 - row_number()"
        INT total_versions "(버전 조회 시) source_file별 총 버전 수"
        BOOLEAN is_latest_version "(버전 조회 시) 최신 버전 여부"
    }

    product_bronze_documents {
        STRING document_id PK "문서 고유 식별자 (source_file_name 확장자 제거)"
        STRING source_file_name "원본 파일명"
        STRING source_file "S3 전체 경로"
        BINARY content "MD 바이너리 데이터"
        LONG file_size_bytes "파일 크기(바이트)"
        TIMESTAMP file_modified_at "파일 수정 시각"
        TIMESTAMP ingested_at "수집 시각"
        STRING bronze_layer "레이어 식별자"
    }

    product_silver_documents {
        STRING document_id PK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING source_file "S3 전체 경로"
        STRING full_text "MD 원문 텍스트 (content를 STRING으로 캐스팅)"
        STRING figure_descriptions "항상 NULL (PDF 전용, MD 전환으로 비활성)"
        INT page_count "항상 NULL (PDF 전용, MD 전환으로 비활성)"
        VARIANT parsed_content "항상 NULL (ai_parse_document 미사용)"
        LONG file_size_bytes "파일 크기(바이트)"
        TIMESTAMP file_modified_at "파일 수정 시각"
        TIMESTAMP ingested_at "수집 시각"
        TIMESTAMP processed_at "처리 완료 시각"
        STRING silver_layer "레이어 식별자"
    }

    product_silver_document_chunks {
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING element_type "항상 'text' (MD 전환 - PDF 복귀 시 table/figure 등 다양화)"
        INT element_page "항상 NULL (MD 전환으로 페이지 개념 없음)"
        INT element_idx "항상 0 (문서 전체를 단일 요소로 취급)"
        INT chunk_idx "요소 내 청크 인덱스"
        STRING chunk_content "청크 텍스트 내용"
        STRING chunk_type "청크 유형(single/overlap)"
        TIMESTAMP chunked_at "청킹 처리 시각"
    }

    product_gold_document_ai_summary {
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING extraction_method "추출 방식 (예: LLM 추출 - 청크 본문 기반)"
        STRING summary "AI 생성 문서 요약"
        ARRAY_STRING keywords "AI 추출 키워드 태그 목록"
        VARIANT metadata "JSON 형태 메타데이터 (아래 예시 참조)"
        TIMESTAMP generated_at "요약 생성 시각"
    }

    product_gold_document_embeddings {
        STRING chunk_id PK "청크 고유 식별자"
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING element_type "요소 타입"
        INT element_page "요소가 위치한 페이지"
        INT element_idx "페이지 내 요소 인덱스"
        INT chunk_idx "요소 내 청크 인덱스"
        STRING chunk_content "청크 텍스트 내용"
        STRING chunk_type "청크 유형"
        TIMESTAMP chunked_at "청킹 처리 시각"
    }

    product_gold_document_embeddings_index {
        STRING chunk_id PK "청크 고유 식별자"
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING element_type "요소 타입"
        INT element_page "요소가 위치한 페이지"
        INT element_idx "페이지 내 요소 인덱스"
        STRING chunk_content "청크 텍스트 내용"
        STRING chunk_type "청크 유형"
        TIMESTAMP chunked_at "청킹 처리 시각"
        ARRAY_FLOAT __db_chunk_content_vector "임베딩 벡터(qwen3-embedding-0-6b, 1024d)"
    }

    staging_documents ||--|| product_bronze_documents : "1:1 source_file join (product_path 필터)"
    product_bronze_documents ||--|| product_silver_documents : "1:1 텍스트 디코딩 (content.cast(STRING))"
    product_silver_documents ||--o{ product_silver_document_chunks : "1:N Chunk (문서 전체 오버랩 청킹)"
    product_silver_documents ||--|| product_gold_document_ai_summary : "1:1 AI Summary"
    product_silver_document_chunks ||--|| product_gold_document_embeddings : "1:1 chunk_id 할당"
    product_gold_document_embeddings ||--|| product_gold_document_embeddings_index : "1:1 Vector Sync (Delta Sync, 상품별 인덱스)"
```

> **참고(상품별 반복)**: 위 ERD는 상품 하나에 대한 템플릿이며, `product_`로 시작하는 엔티티명은 실제로는 `{상품명}_`가 붙습니다(예: `product_bronze_documents` → `종신보험_bronze_documents`). `config.get_product_list()`가 반환하는 상품마다 `bronze_documents.py` / `silver_documents.py` / `silver_document_chunks.py` / `gold_document_ai_summary.py` / `gold_document_embeddings.py`가 각각 for 루프로 실제 테이블 세트를 생성하므로, 실제 스키마에는 상품 수 × 5개(+ Vector Search 인덱스)의 물리 테이블이 존재합니다. `staging_documents`(및 `staging_document_versions`)만 전 상품 공통 단일 테이블입니다.

> **참고**: `staging_documents`, `{상품명}_silver_document_chunks`, `{상품명}_gold_document_ai_summary`는 코드상 명시적 PK 제약이 선언되어 있지 않습니다. `{상품명}_bronze_documents`, `{상품명}_silver_documents`, `{상품명}_gold_document_embeddings`만 스키마에 `CONSTRAINT ... PRIMARY KEY`가 명시되어 있습니다.
>
> **참고(버전 조회)**: `version_number` / `total_versions` / `is_latest_version`은 `staging_documents` 테이블 자체 컬럼이 아니라, 이를 `source_file` 기준으로 읽어 `row_number()`를 계산하는 별도 Materialized View(`staging_document_versions`)가 파생시키는 값입니다. 두 객체는 물리적으로 분리되어 있지만 하나의 논리적 엔티티로 묶어 설명합니다.
>
> **참고(S3 버전 관리)**: S3 버킷 버저닝은 콘솔에서 활성화되어 있으나, 위 버전 컬럼들은 메타데이터(도착 순서/최신 여부)만 추적할 뿐 과거 버전의 실제 파일 콘텐츠는 저장/대조하지 않습니다. S3 VersionId와 연동한 콘텐츠 히스토리 조회는 범위 외(TODO)입니다.

---

## Data Flow Diagram

```mermaid
flowchart TD
    subgraph Staging["Staging Layer"]
        ST["staging_documents<br/><i>Streaming Table</i><br/>+ 버전 조회 MV(staging_document_versions)"]
    end

    subgraph Bronze["Bronze Layer (상품별 × N)"]
        B["{product}_bronze_documents<br/><i>Streaming Table</i>"]
    end

    subgraph Silver["Silver Layer (상품별 × N)"]
        S["{product}_silver_documents<br/><i>Streaming Table</i>"]
        SC["{product}_silver_document_chunks<br/><i>Streaming Table</i>"]
    end

    subgraph Gold["Gold Layer (상품별 × N)"]
        GAS["{product}_gold_document_ai_summary<br/><i>Streaming Table</i>"]
        GE["{product}_gold_document_embeddings<br/><i>Streaming Table</i>"]
    end

    subgraph VectorSearch["Vector Search (상품별 × N)"]
        GEI["{product}_gold_document_embeddings_index<br/><i>Vector Index - Delta Sync</i>"]
    end

    S3[(S3 Landing Zone<br/>MD Files, 상품별 폴더)] -->|Auto Loader<br/>메타데이터만| ST
    S3 -->|binaryFile static read<br/>product_path만| B
    ST -->|stream-static join<br/>source_file.startswith product_path| B
    B -->|content.cast STRING| S
    S -->|문서 전체 오버랩 청킹| SC
    S -->|ai_query 요약/키워드/메타데이터| GAS
    SC -->|chunk_id 할당| GE
    GE -->|Delta Sync| GEI

    style Staging fill:#E6F7FF,stroke:#1D39C4,stroke-width:2px
    style Bronze fill:#FFF7E6,stroke:#D48806,stroke-width:2px
    style Silver fill:#F5F5F5,stroke:#595959,stroke-width:2px
    style Gold fill:#FFFBE6,stroke:#B8860B,stroke-width:2px
    style VectorSearch fill:#F0E6FF,stroke:#531DAB,stroke-width:2px
```

---

## Table Details

### 1. staging_documents

| Property | Value |
|----------|-------|
| Type | Streaming Table |
| Layer | Staging |
| Source | S3 Landing Zone (Auto Loader, `cloudFiles.allowOverwrites=true`, `recursiveFileLookup=true`) |
| Description | S3 파일 메타데이터 및 버전 이력 추적 (append-only 이벤트 원장, 바이너리 콘텐츠는 저장하지 않음) |
| Primary Key | 없음 (`source_file`이 사실상 자연키) |

> **참고(상품별 분기 방식)**: `staging_documents.py` 상단 docstring은 `product_name` 컬럼을 추출해 그 컬럼 기준으로 분기하는 방식을 언급하지만, 실제 구현은 `staging_documents`에 컬럼을 추가하는 대신 **bronze 레이어에서 `source_file` 경로 접두사로 직접 필터링**하는 방식을 사용합니다. `config.get_product_list()`가 파이프라인 그래프 빌드 시점에 S3 Landing Zone(`보험/` 바로 아래 1뎁스 폴더)을 스캔해 상품 목록을 얻고, `bronze_documents.py`/`silver_documents.py`/`silver_document_chunks.py`/`gold_document_ai_summary.py`/`gold_document_embeddings.py`가 각각 그 목록으로 for 루프를 돌며 `{상품명}_bronze_documents`, `{상품명}_silver_documents` 등 상품별 테이블 세트를 생성합니다. `staging_documents` 자체는 상품 구분 없이 전 상품 공통 스키마·단일 테이블을 유지합니다.

**버전 조회 (staging_document_versions)**: `staging_documents`는 append-only 이벤트 원장이라 재업로드된 파일도 새 행으로만 쌓입니다. 이를 `source_file` 기준으로 묶어 도착 순서(`version_number`)와 최신 버전 여부(`is_latest_version`)를 보기 쉽게 만드는 별도 Materialized View `staging_document_versions`(`보험.staging`, PK 없음)가 함께 정의되어 있습니다. 물리적으로는 별개 객체이지만 `staging_documents`를 그대로 읽어 `row_number()`/`max()` 윈도우 함수만 얹은 파생 뷰이므로 이 문서에서는 하나로 묶어 설명합니다.

> **TODO**: 버전은 메타데이터(도착 이력) 기준으로만 관리되며, 과거 버전의 실제 파일 바이너리 콘텐츠는 저장/대조하지 않습니다. S3 버킷 버저닝(콘솔에서 설정 완료)의 VersionId와 연동한 실제 콘텐츠 히스토리 조회는 이번 범위에서 제외 — 필요 시 별도 작업으로 진행.

### 2. {상품명}_bronze_documents

| Property | Value |
|----------|-------|
| Type | Streaming Table (상품별 동적 생성, `bronze_documents.py`의 `for _product in config.get_product_list(): _generate_bronze_documents(_product)`) |
| Layer | Bronze |
| Source | `staging_documents` (stream, `source_file.startswith(product_path)` 필터) + S3 binaryFile (static, `product_path`만 `recursiveFileLookup=true`) |
| Description | 해당 상품 폴더의 문서 파일(MD)을 수집한 원시 바이너리 데이터 |
| Primary Key | `document_id` (제약명 `` `pk_{상품명}_bronze_documents` ``) |

**`document_id` 생성 규칙**: `source_file_name`(경로 제외 파일명)에서 마지막 확장자만 정규식(`\.[^.]+$`)으로 제거한 값을 그대로 사용합니다 (예: `agreement_v1.md` → `agreement_v1`). 경로(`source_file`)는 사용하지 않으므로 같은 상품 폴더 내 서로 다른 하위 폴더에 동일 파일명이 존재하거나 파일이 재업로드(버전 갱신)되면 동일한 `document_id`가 재사용되어 PK 제약과 충돌/갱신될 수 있습니다. 다른 상품끼리는 테이블 자체가 분리되어 있어 충돌하지 않습니다.

> **TODO**: 현재는 매 트리거마다 S3의 "현재" 바이너리를 static join으로 읽어오므로, `staging_document_versions`에 기록된 과거 버전의 실제 파일 내용(바이너리)은 보존되지 않습니다. S3 버킷 버저닝의 VersionId와 연동해 과거 버전 콘텐츠까지 조회/대조하는 기능은 이번 범위에서 제외 — 필요 시 별도 작업으로 진행.

### 3. {상품명}_silver_documents

| Property | Value |
|----------|-------|
| Type | Streaming Table (상품별 동적 생성) |
| Layer | Silver |
| Source | `{상품명}_bronze_documents` |
| Description | 해당 상품 MD 파일 텍스트를 추출·정제한 데이터 (`ai_parse_document()` 미사용) |
| Primary Key | `document_id` (제약명 `` `pk_{상품명}_silver_documents` ``) |

`full_text`는 `content`를 `STRING`으로 캐스팅한 MD 원문이며, `figure_descriptions` / `page_count` / `parsed_content`는 PDF 전용 필드로 MD 전환 후 항상 `NULL`입니다. 기존 `ai_parse_document()` 기반 파싱 로직(figure 설명 추출, HTML 표 태그 정리, `error_status` 필터링 등)은 삭제하지 않고 주석 처리로 보존되어 있으며, 현재는 `full_text`가 비어있지 않은 문서만 필터링합니다.

### 4. {상품명}_silver_document_chunks

| Property | Value |
|----------|-------|
| Type | Streaming Table (상품별 동적 생성) |
| Layer | Silver |
| Source | `{상품명}_silver_documents` |
| Description | 문서 전체(`full_text`)를 단일 요소로 오버랩 청킹 - RAG 벡터검색용 (`overlap_chunk` Python UDF, AI 미사용) |
| Foreign Key | `document_id` → `{상품명}_silver_documents.document_id` (제약명 `` `fk_{상품명}_chunks_document` ``) |

기존에는 `ai_parse_document()` 결과의 요소(text/table/figure)별로 청킹했으나, MD 전환으로 요소 구분이 사라져 `element_type="text"`, `element_page=NULL`, `element_idx=0` 고정값으로 문서 전체를 단일 요소 취급합니다. 청킹 자체도 기존 SQL `sequence`/`substr` 기반 방식에서 `overlap_chunk` UDF(Python 슬라이딩 윈도우, `chunk_index`/`chunk_text`/`start_offset`/`end_offset` 반환)로 대체되었습니다. 두 기존 로직 모두 삭제하지 않고 주석 처리로 보존되어 있습니다.

### 5. {상품명}_gold_document_ai_summary

| Property | Value |
|----------|-------|
| Type | Streaming Table (상품별 동적 생성) |
| Layer | Gold |
| Source | `{상품명}_silver_documents` |
| Description | 해당 상품 문서별 AI 요약 및 메타데이터 (생명보험 도메인) |
| Foreign Key | `document_id` → `{상품명}_silver_documents.document_id` (제약명 `` `fk_{상품명}_summary_document` ``) |

**`metadata` 컬럼 JSON 구조 예시** (현재 `ai_query()` 프롬프트가 요청하는 필드 기준):

```json
{
  "doc_category": "약관",
  "insurance_product_type": "종신보험",
  "related_systems": ["보험코어", "언더라이팅", "CRM"],
  "sensitivity_level": "내부",
  "regulation_reference": "보험업법 제127조"
}
```

### 6. {상품명}_gold_document_embeddings

| Property | Value |
|----------|-------|
| Type | Streaming Table (상품별 동적 생성) |
| Layer | Gold |
| Source | `{상품명}_silver_document_chunks` |
| Description | 해당 상품 Vector Search 소스 테이블 (임베딩은 Vector Search가 자동 계산) |
| Primary Key | `chunk_id` (제약명 `` `pk_{상품명}_gold_embeddings` ``) |
| Foreign Key | `document_id` → `{상품명}_silver_documents.document_id` (제약명 `` `fk_{상품명}_embeddings_document` ``) |
| CDF | `delta.enableChangeDataFeed = true` |

### 7. {상품명}_gold_document_embeddings_index

| Property | Value |
|----------|-------|
| Type | Foreign Table (Vector Index) |
| Layer | Gold |
| Source | `{상품명}_gold_document_embeddings` (Delta Sync) |
| Description | 해당 상품 Managed Vector Index - chunk_content 컬럼에서 벡터 자동 생성 |
| Primary Key | `chunk_id` |
| Embedding Model | `databricks-qwen3-embedding-0-6b` (1024 dimensions) |
| Endpoint | `document-search-endpoint` (STANDARD, 상품 간 공용) |

> **참고**: 이 인덱스는 파이프라인 `.py` 코드가 아니라 별도 노트북/스크립트에서 `VectorSearchClient.create_delta_sync_index()`로 생성합니다 (`gold_document_embeddings.py` 상단 docstring 참조). 상품별 인덱스를 만들려면 `config.get_product_list()`로 상품마다 반복 호출해야 하며, 이 자동화는 현재 리포지토리에 포함되어 있지 않습니다.

---

## Lineage Summary

```
S3 (MD, 상품별 1뎁스 폴더: 보험/{상품명}/...)
 └── staging_documents (전 상품 공통)   [메타데이터만, append-only 이벤트 원장, 바이너리 미저장]
      │    (+ 버전 조회 MV staging_document_versions: source_file별 version_number / is_latest_version)
      └── (상품마다 반복: config.get_product_list())
           └── {상품명}_bronze_documents      [source_file.startswith(product_path) 필터 + stream-static join]
                └── {상품명}_silver_documents  [content.cast(STRING), ai_parse_document 미사용]
                     ├── {상품명}_silver_document_chunks     [문서 전체 오버랩 청킹 - overlap_chunk UDF]
                     │    └── {상품명}_gold_document_embeddings  [chunk_id 할당]
                     │         └── {상품명}_gold_document_embeddings_index  [Vector Search Delta Sync, 별도 스크립트로 생성]
                     └── {상품명}_gold_document_ai_summary   [ai_query 요약/키워드/메타데이터]
```
