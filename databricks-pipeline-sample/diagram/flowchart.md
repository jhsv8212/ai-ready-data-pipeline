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