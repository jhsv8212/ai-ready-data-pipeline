# dev_haesung — 데이터 파이프라인 아키텍처

## Overview

- **Catalog:** `dev_haesung`
- **Schemas:** `staging`, `bronze`, `silver`, `gold`
- **Pipeline:** `databricks-pipeline-sample` (보험 문서 RAG 파이프라인)
- **Pipeline ID:** `454e3aef-72c9-48da-80f7-4910933e8b4e`
- **Last updated:** 2026-08-23 (staging 레이어 반영)

S3 Landing Zone에 적재된 보험 PDF 문서를 Auto Loader로 수집하고, Databricks AI 함수(`ai_parse_document`, `ai_query`)로 텍스트를 추출·요약·청킹한 뒤 Vector Search 인덱스와 동기화하여 RAG(Retrieval-Augmented Generation)에 활용하는 Lakeflow Declarative Pipeline입니다.

---

## 파이프라인 시퀀스 흐름 (Mermaid)

File Arrival 트리거부터 Vector Search 동기화까지의 처리 순서입니다.

```mermaid
sequenceDiagram
    autonumber
    participant S3 as S3 Landing Zone
    participant Job as Lakeflow Job<br/>(File Arrival Trigger)
    participant Staging as staging_documents
    participant Bronze as bronze_documents
    participant Silver as silver_documents
    participant LLM as Model Serving<br/>(ai_parse_document / ai_query)
    participant Chunks as silver_document_chunks
    participant Summary as gold_document_ai_summary
    participant Embeddings as gold_document_embeddings
    participant VS as Vector Search Index

    Note over S3: PDF 업로드
    S3->>Job: 파일 도착 감지
    Note over Job: 마지막 파일 변경 후 300초 대기<br/>최소 실행 간격 600초
    Job->>Staging: 파이프라인 업데이트 시작

    Staging->>S3: Auto Loader (cloudFiles, binaryFile)
    Staging->>Staging: 파일 메타데이터 수집<br/>(allowOverwrites=true, 바이너리 미저장)

    Staging->>Bronze: staging_documents 스트림 read
    Bronze->>S3: binaryFile static read (바이너리 콘텐츠)
    Bronze->>Bronze: stream-static join<br/>(source_file = path)

    Bronze->>Silver: bronze_documents 스트림 read
    Silver->>LLM: ai_parse_document(content, version=2.0)
    LLM-->>Silver: parsed_content (VARIANT)
    Silver->>Silver: full_text / figure_descriptions 추출<br/>error_status 문서 필터링

    par Gold - AI 요약 / 키워드 / 메타데이터
        Silver->>Summary: silver_documents 스트림 read
        Summary->>LLM: ai_query() 요약 생성
        Summary->>LLM: ai_query() 키워드 추출
        Summary->>LLM: ai_query() 메타데이터(JSON) 추출
        LLM-->>Summary: summary, keywords, metadata
    and Silver/Gold - 청킹 및 임베딩 소스
        Silver->>Chunks: silver_documents 스트림 read
        Chunks->>Chunks: 요소별 오버랩 청킹<br/>(알고리즘 기반, AI 미사용 / CHUNK_SIZE=500, OVERLAP=100)
        Chunks->>Embeddings: silver_document_chunks 스트림 read
        Embeddings->>Embeddings: chunk_id = md5(document_id, element_idx, chunk_idx)
    end

    Embeddings->>VS: Delta Sync (CDF 기반)
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
    }

    bronze_documents {
        STRING document_id PK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING source_file "S3 전체 경로"
        BINARY content "PDF 바이너리 데이터"
        LONG file_size_bytes "파일 크기(바이트)"
        TIMESTAMP file_modified_at "파일 수정 시각"
        TIMESTAMP ingested_at "수집 시각"
        STRING bronze_layer "레이어 식별자"
    }

    silver_documents {
        STRING document_id PK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING source_file "S3 전체 경로"
        STRING full_text "추출된 전체 텍스트"
        STRING figure_descriptions "이미지/도표 설명"
        INT page_count "총 페이지 수"
        VARIANT parsed_content "파싱된 구조화 데이터(JSON)"
        LONG file_size_bytes "파일 크기(바이트)"
        TIMESTAMP file_modified_at "파일 수정 시각"
        TIMESTAMP ingested_at "수집 시각"
        TIMESTAMP processed_at "파싱 완료 시각"
        STRING silver_layer "레이어 식별자"
    }

    silver_document_chunks {
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING element_type "요소 타입(text/table/figure)"
        INT element_page "요소가 위치한 페이지"
        INT element_idx "페이지 내 요소 인덱스"
        INT chunk_idx "요소 내 청크 인덱스"
        STRING chunk_content "청크 텍스트 내용"
        STRING chunk_type "청크 유형(single/overlap)"
        TIMESTAMP chunked_at "청킹 처리 시각"
    }

    gold_document_ai_summary {
        STRING document_id FK "문서 고유 식별자"
        STRING source_file_name "원본 파일명"
        STRING extraction_method "추출 방식 (예: LLM 추출 - 청크 본문 기반)"
        STRING summary "AI 생성 문서 요약"
        ARRAY_STRING keywords "AI 추출 키워드 태그 목록"
        VARIANT metadata "JSON 형태 메타데이터 (아래 예시 참조)"
        TIMESTAMP generated_at "요약 생성 시각"
    }

    gold_document_embeddings {
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

    gold_document_embeddings_index {
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

    staging_documents ||--|| bronze_documents : "1:1 source_file join"
    bronze_documents ||--|| silver_documents : "1:1 Parse (ai_parse_document)"
    silver_documents ||--o{ silver_document_chunks : "1:N Chunk (요소별 오버랩 청킹)"
    silver_documents ||--|| gold_document_ai_summary : "1:1 AI Summary"
    silver_document_chunks ||--|| gold_document_embeddings : "1:1 chunk_id 할당"
    gold_document_embeddings ||--|| gold_document_embeddings_index : "1:1 Vector Sync (Delta Sync)"
```

> **참고**: `staging_documents`, `silver_document_chunks`, `gold_document_ai_summary`는 코드상 명시적 PK 제약이 선언되어 있지 않습니다. `bronze_documents`, `silver_documents`, `gold_document_embeddings`만 스키마에 `CONSTRAINT ... PRIMARY KEY`가 명시되어 있습니다.

---

## Data Flow Diagram

```mermaid
flowchart TD
    subgraph Staging["Staging Layer"]
        ST[staging_documents<br/><i>Streaming Table</i>]
    end

    subgraph Bronze["Bronze Layer"]
        B[bronze_documents<br/><i>Streaming Table</i>]
    end

    subgraph Silver["Silver Layer"]
        S[silver_documents<br/><i>Streaming Table</i>]
        SC[silver_document_chunks<br/><i>Streaming Table</i>]
    end

    subgraph Gold["Gold Layer"]
        GAS[gold_document_ai_summary<br/><i>Streaming Table</i>]
        GE[gold_document_embeddings<br/><i>Streaming Table</i>]
    end

    subgraph VectorSearch["Vector Search"]
        GEI[gold_document_embeddings_index<br/><i>Vector Index - Delta Sync</i>]
    end

    S3[(S3 Landing Zone<br/>PDF Files)] -->|Auto Loader<br/>메타데이터만| ST
    S3 -->|binaryFile static read| B
    ST -->|stream-static join| B
    B -->|ai_parse_document| S
    S -->|요소별 오버랩 청킹| SC
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
| Source | S3 Landing Zone (Auto Loader, `cloudFiles.allowOverwrites=true`) |
| Description | S3 파일 메타데이터 및 버전 이력 추적 (바이너리 콘텐츠는 저장하지 않음) |
| Primary Key | 없음 (`source_file`이 사실상 자연키) |

### 2. bronze_documents

| Property | Value |
|----------|-------|
| Type | Streaming Table |
| Layer | Bronze |
| Source | `staging_documents` (stream) + S3 binaryFile (static) |
| Description | S3에서 PDF 파일을 수집한 원시 바이너리 데이터 |
| Primary Key | `document_id` |

### 3. silver_documents

| Property | Value |
|----------|-------|
| Type | Streaming Table |
| Layer | Silver |
| Source | `bronze_documents` |
| Description | ai_parse_document()로 PDF 텍스트를 추출·정제한 데이터 |
| Primary Key | `document_id` |

### 4. silver_document_chunks

| Property | Value |
|----------|-------|
| Type | Streaming Table |
| Layer | Silver |
| Source | `silver_documents` |
| Description | 문서 요소별 오버랩 텍스트 청킹 - RAG 벡터검색용 (AI 미사용, 고정 길이 슬라이딩 윈도우) |
| Foreign Key | `document_id` → `silver_documents.document_id` |

### 5. gold_document_ai_summary

| Property | Value |
|----------|-------|
| Type | Streaming Table |
| Layer | Gold |
| Source | `silver_documents` |
| Description | 문서별 AI 요약 및 메타데이터 (생명보험 도메인) |
| Foreign Key | `document_id` → `silver_documents.document_id` |

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

### 6. gold_document_embeddings

| Property | Value |
|----------|-------|
| Type | Streaming Table |
| Layer | Gold |
| Source | `silver_document_chunks` |
| Description | Vector Search 소스 테이블 (임베딩은 Vector Search가 자동 계산) |
| Primary Key | `chunk_id` |
| Foreign Key | `document_id` → `silver_documents.document_id` |
| CDF | `delta.enableChangeDataFeed = true` |

### 7. gold_document_embeddings_index

| Property | Value |
|----------|-------|
| Type | Foreign Table (Vector Index) |
| Layer | Gold |
| Source | `gold_document_embeddings` (Delta Sync) |
| Description | Managed Vector Index - chunk_content 컬럼에서 벡터 자동 생성 |
| Primary Key | `chunk_id` |
| Embedding Model | `databricks-qwen3-embedding-0-6b` (1024 dimensions) |
| Endpoint | `document-search-endpoint` (STANDARD) |

---

## Lineage Summary

```
S3 (PDF)
 └── staging_documents              [메타데이터만, 바이너리 미저장]
      └── bronze_documents          [stream-static join]
           └── silver_documents     [ai_parse_document]
                ├── silver_document_chunks     [요소별 오버랩 청킹]
                │    └── gold_document_embeddings  [chunk_id 할당]
                │         └── gold_document_embeddings_index  [Vector Search Delta Sync]
                └── gold_document_ai_summary   [ai_query 요약/키워드/메타데이터]
```
