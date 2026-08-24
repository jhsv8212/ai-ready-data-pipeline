# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # dev_haesung.default ERD (Entity Relationship Diagram)
# MAGIC
# MAGIC **Schema:** `dev_haesung.default`  
# MAGIC **Pipeline:** `databricks-pipeline-sample` (Document Processing Pipeline)
# MAGIC
# MAGIC ## Tables (7)
# MAGIC | Layer | Table | Type | Description |
# MAGIC |-------|-------|------|-------------|
# MAGIC | Staging | `staging_documents` | Streaming Table | S3 Landing Zone 파일 메타데이터 및 버전 이력 |
# MAGIC | Bronze | `bronze_documents` | Streaming Table | S3 Landing Zone에서 문서(MD) 바이너리 수집 |
# MAGIC | Silver | `silver_documents` | Streaming Table | MD 텍스트 추출/정제 (ai_parse_document 미사용) |
# MAGIC | Silver | `silver_document_chunks` | Streaming Table | 요소별 오버랩 청킹 (overlap_chunk UDF, RAG 벡터검색용) |
# MAGIC | Gold | `gold_document_ai_summary` | Streaming Table | AI 기반 문서 요약 및 키워드 추출 |
# MAGIC | Gold | `gold_document_embeddings` | Streaming Table | Vector Search 소스 테이블 (CDF 활성화) |
# MAGIC | Gold | `gold_document_embeddings_index` | Vector Index | Delta Sync 기반 벡터 인덱스 (qwen3-embedding-0-6b, 1024d) |
# MAGIC
# MAGIC ## Relationships
# MAGIC - `staging_documents` → `bronze_documents` : 1:1 (source_file 기준 stream-static join)
# MAGIC - `bronze_documents` → `silver_documents` : 1:1 (Parse)
# MAGIC - `silver_documents` → `silver_document_chunks` : 1:N (Chunk)
# MAGIC - `silver_documents` → `gold_document_ai_summary` : 1:1 (AI Summary)
# MAGIC - `silver_document_chunks` → `gold_document_embeddings` : 1:N (chunk_id 할당)
# MAGIC - `gold_document_embeddings` → `gold_document_embeddings_index` : 1:1 (Vector Sync)
# MAGIC
# MAGIC > 참고: 아래 matplotlib 시각화(`tables`/`positions` dict)는 staging_documents를 포함하지 않은
# MAGIC > 기존 6-테이블 레이아웃입니다. PNG 다이어그램에 staging 레이어를 추가하려면 레이아웃 좌표를
# MAGIC > 함께 재계산해야 하므로, 정확한 전체 스키마는 `dev_haesung_default_ERD.md`를 참고하세요.

# COMMAND ----------

# DBTITLE 1,Install graphviz
# =============================================================================
# Dependencies
# =============================================================================
# matplotlib: ERD 다이어그램 렌더링에 사용 (Serverless 환경 기본 설치됨)
# graphviz 시스템 바이너리가 Serverless에 없으므로 순수 matplotlib로 구현
# =============================================================================

# COMMAND ----------

# DBTITLE 1,dev_haesung.default ERD Visualization
# =============================================================================
# ERD Visualization - dev_haesung.default
# =============================================================================
# 이 셀은 dev_haesung.default 스키마의 전체 테이블 구조와 관계를
# matplotlib로 시각화합니다.
#
# 구성:
#   1) 테이블 정의 (tables dict) - 각 테이블의 컨럼, 타입, PK/FK 정보
#   2) 레이아웃 위치 (positions) - ERD 상의 x, y 좌표
#   3) 드로잉 함수 - 테이블 박스와 관계선 렌더링
#   4) 차트 생성 및 출력
# =============================================================================

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe

# === Table Definitions ===
# 각 테이블의 메타데이터: 타입, 레이어, 색상, 컨럼(PK/FK 포함)
tables = {
    'bronze_documents': {
        'type': 'Streaming Table', 'layer': 'Bronze',
        'color': '#D48806', 'header_bg': '#D48806',
        'columns': [
            ('document_id', 'STRING', 'PK'),
            ('source_file_name', 'STRING', ''),
            ('source_file', 'STRING', ''),
            ('content', 'BINARY', ''),
            ('file_size_bytes', 'LONG', ''),
            ('file_modified_at', 'TIMESTAMP', ''),
            ('ingested_at', 'TIMESTAMP', ''),
            ('bronze_layer', 'STRING', ''),
        ]
    },
    'silver_documents': {
        'type': 'Streaming Table', 'layer': 'Silver',
        'color': '#595959', 'header_bg': '#595959',
        'columns': [
            ('document_id', 'STRING', 'PK'),
            ('source_file_name', 'STRING', ''),
            ('source_file', 'STRING', ''),
            ('full_text', 'STRING', ''),
            ('figure_descriptions', 'STRING', ''),
            ('page_count', 'INT', ''),
            ('parsed_content', 'VARIANT', ''),
            ('file_size_bytes', 'LONG', ''),
            ('file_modified_at', 'TIMESTAMP', ''),
            ('ingested_at', 'TIMESTAMP', ''),
            ('processed_at', 'TIMESTAMP', ''),
            ('silver_layer', 'STRING', ''),
        ]
    },
    'silver_document_chunks': {
        'type': 'Streaming Table', 'layer': 'Silver',
        'color': '#595959', 'header_bg': '#595959',
        'columns': [
            ('document_id', 'STRING', 'FK'),
            ('source_file_name', 'STRING', ''),
            ('element_type', 'STRING', ''),
            ('element_page', 'INT', ''),
            ('element_idx', 'INT', ''),
            ('chunk_idx', 'INT', ''),
            ('chunk_content', 'STRING', ''),
            ('chunk_type', 'STRING', ''),
            ('chunked_at', 'TIMESTAMP', ''),
        ]
    },
    'gold_document_ai_summary': {
        'type': 'Streaming Table', 'layer': 'Gold',
        'color': '#B8860B', 'header_bg': '#B8860B',
        'columns': [
            ('document_id', 'STRING', 'FK'),
            ('source_file_name', 'STRING', ''),
            ('extraction_method', 'STRING', ''),
            ('summary', 'STRING', ''),
            ('keywords', 'ARRAY<STRING>', ''),
            ('metadata', 'VARIANT', ''),
            ('generated_at', 'TIMESTAMP', ''),
        ]
    },
    'gold_document_embeddings': {
        'type': 'Streaming Table', 'layer': 'Gold',
        'color': '#B8860B', 'header_bg': '#B8860B',
        'columns': [
            ('chunk_id', 'STRING', 'PK'),
            ('document_id', 'STRING', 'FK'),
            ('source_file_name', 'STRING', ''),
            ('element_type', 'STRING', ''),
            ('element_page', 'INT', ''),
            ('element_idx', 'INT', ''),
            ('chunk_idx', 'INT', ''),
            ('chunk_content', 'STRING', ''),
            ('chunk_type', 'STRING', ''),
            ('chunked_at', 'TIMESTAMP', ''),
        ]
    },
    'gold_document_embeddings_index': {
        'type': 'Vector Index', 'layer': 'Gold',
        'color': '#531DAB', 'header_bg': '#531DAB',
        'columns': [
            ('chunk_id', 'STRING', 'PK'),
            ('document_id', 'STRING', 'FK'),
            ('source_file_name', 'STRING', ''),
            ('element_type', 'STRING', ''),
            ('element_page', 'INT', ''),
            ('element_idx', 'INT', ''),
            ('chunk_content', 'STRING', ''),
            ('chunk_type', 'STRING', ''),
            ('chunked_at', 'TIMESTAMP', ''),
            ('__db_chunk_content_vector', 'ARRAY<FLOAT>', ''),
        ]
    },
}

# === Layout positions (x, y) ===
# ERD 다이어그램 내 각 테이블의 좌상단 좌표
# 좌측(x=0.5): Bronze → Silver → Gold (Chunks) 위에서 아래로
# 우측(x=5.5): Vector Index → Embeddings → AI Summary 위에서 아래로
positions = {
    'bronze_documents': (0.5, 8.5),
    'silver_documents': (0.5, 5.0),
    'silver_document_chunks': (0.5, 1.2),
    'gold_document_ai_summary': (5.5, 1.2),
    'gold_document_embeddings': (5.5, 5.0),
    'gold_document_embeddings_index': (5.5, 8.5),
}

# === Drawing Functions ===
# draw_table(): 테이블 박스를 그림 (header + columns + PK/FK 배지)
# draw_relationship(): 테이블 간 관계선과 카디널리티 라벨을 그림
def draw_table(ax, x, y, table_name, table_info):
    cols = table_info['columns']
    header_bg = table_info['header_bg']
    table_type = table_info['type']
    
    width = 4.2
    row_height = 0.28
    header_height = 0.55
    total_height = header_height + len(cols) * row_height + 0.1
    
    # Shadow
    shadow = FancyBboxPatch((x + 0.03, y - total_height - 0.03), width, total_height,
                            boxstyle="round,pad=0.05", facecolor='#e0e0e0', edgecolor='none', zorder=1)
    ax.add_patch(shadow)
    
    # Main box
    box = FancyBboxPatch((x, y - total_height), width, total_height,
                         boxstyle="round,pad=0.05", facecolor='white', edgecolor=header_bg, linewidth=1.5, zorder=2)
    ax.add_patch(box)
    
    # Header
    header = FancyBboxPatch((x, y - header_height), width, header_height,
                            boxstyle="round,pad=0.05", facecolor=header_bg, edgecolor=header_bg, linewidth=1.5, zorder=3)
    ax.add_patch(header)
    # Clip bottom corners of header
    header_rect = plt.Rectangle((x, y - header_height), width, header_height * 0.5,
                                facecolor=header_bg, edgecolor='none', zorder=3)
    ax.add_patch(header_rect)
    
    # Header text
    ax.text(x + width/2, y - 0.18, table_name, fontsize=8, fontweight='bold',
            color='white', ha='center', va='center', zorder=4, fontfamily='monospace')
    ax.text(x + width/2, y - 0.42, f'[{table_type}]', fontsize=6,
            color='#ffffffcc', ha='center', va='center', zorder=4)
    
    # Columns
    for i, (col_name, col_type, key) in enumerate(cols):
        cy = y - header_height - 0.05 - (i + 0.5) * row_height
        
        # Alternating row background
        if i % 2 == 0:
            row_bg = plt.Rectangle((x + 0.05, cy - row_height/2), width - 0.1, row_height,
                                   facecolor='#fafafa', edgecolor='none', zorder=2.5)
            ax.add_patch(row_bg)
        
        # Key badge
        col_x = x + 0.15
        if key == 'PK':
            ax.text(col_x, cy, 'PK', fontsize=5.5, fontweight='bold', color='#D4380D',
                    ha='left', va='center', zorder=4,
                    bbox=dict(boxstyle='round,pad=0.1', facecolor='#FFF1F0', edgecolor='#FFA39E', linewidth=0.5))
            col_x += 0.4
        elif key == 'FK':
            ax.text(col_x, cy, 'FK', fontsize=5.5, fontweight='bold', color='#1D39C4',
                    ha='left', va='center', zorder=4,
                    bbox=dict(boxstyle='round,pad=0.1', facecolor='#F0F5FF', edgecolor='#ADC6FF', linewidth=0.5))
            col_x += 0.4
        
        ax.text(col_x, cy, col_name, fontsize=6.5, color='#262626',
                ha='left', va='center', zorder=4, fontfamily='monospace')
        ax.text(x + width - 0.15, cy, col_type, fontsize=5.5, color='#8c8c8c',
                ha='right', va='center', zorder=4)
    
    return {
        'x': x, 'y': y, 'width': width, 'height': total_height,
        'center_x': x + width/2,
        'top': y, 'bottom': y - total_height,
        'left': x, 'right': x + width
    }

def draw_relationship(ax, from_box, to_box, label, color, from_side='bottom', to_side='top'):
    """Draw an arrow between two table boxes."""
    if from_side == 'bottom':
        start = (from_box['center_x'], from_box['bottom'])
    elif from_side == 'right':
        start = (from_box['right'], (from_box['top'] + from_box['bottom']) / 2)
    else:
        start = (from_box['center_x'], from_box['top'])
    
    if to_side == 'top':
        end = (to_box['center_x'], to_box['top'])
    elif to_side == 'left':
        end = (to_box['left'], (to_box['top'] + to_box['bottom']) / 2)
    else:
        end = (to_box['center_x'], to_box['bottom'])
    
    arrow = FancyArrowPatch(start, end,
                            arrowstyle='->', mutation_scale=12,
                            color=color, linewidth=2, zorder=5,
                            connectionstyle='arc3,rad=0.0')
    ax.add_patch(arrow)
    
    mid_x = (start[0] + end[0]) / 2
    mid_y = (start[1] + end[1]) / 2
    ax.text(mid_x, mid_y, label, fontsize=6, color=color, fontweight='bold',
            ha='center', va='center', zorder=6,
            bbox=dict(boxstyle='round,pad=0.2', facecolor='white', edgecolor=color, linewidth=0.8, alpha=0.9))

# === Create Figure ===
# 18x14 인치 캔버스에 전체 ERD 렌더링
fig, ax = plt.subplots(1, 1, figsize=(18, 14))
ax.set_xlim(-0.5, 11)
ax.set_ylim(-1, 12)
ax.set_aspect('equal')
ax.axis('off')

# Title
ax.text(5.25, 11.5, 'dev_haesung.default — Entity Relationship Diagram', 
        fontsize=14, fontweight='bold', ha='center', va='center', color='#262626')
ax.text(5.25, 11.1, 'databricks-pipeline-sample (Bronze → Silver → Gold)', 
        fontsize=9, ha='center', va='center', color='#8c8c8c')

# Draw tables
boxes = {}
for name, pos in positions.items():
    boxes[name] = draw_table(ax, pos[0], pos[1], name, tables[name])

# Draw relationships
draw_relationship(ax, boxes['bronze_documents'], boxes['silver_documents'],
                  '1:1 (Parse)', '#D48806', 'bottom', 'top')
draw_relationship(ax, boxes['silver_documents'], boxes['silver_document_chunks'],
                  '1:N (Chunk)', '#595959', 'bottom', 'top')
draw_relationship(ax, boxes['silver_documents'], boxes['gold_document_ai_summary'],
                  '1:1 (AI Summary)', '#595959', 'right', 'left')
draw_relationship(ax, boxes['silver_document_chunks'], boxes['gold_document_embeddings'],
                  '1:N (chunk_id)', '#B8860B', 'right', 'left')
draw_relationship(ax, boxes['gold_document_embeddings'], boxes['gold_document_embeddings_index'],
                  '1:1 (Vector Sync)', '#531DAB', 'top', 'bottom')

# Legend
legend_elements = [
    mpatches.Patch(facecolor='#D48806', label='Bronze Layer'),
    mpatches.Patch(facecolor='#595959', label='Silver Layer'),
    mpatches.Patch(facecolor='#B8860B', label='Gold Layer'),
    mpatches.Patch(facecolor='#531DAB', label='Vector Index'),
]
ax.legend(handles=legend_elements, loc='lower right', fontsize=8, framealpha=0.9)

plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Save ERD as PNG
# =============================================================================
# ERD 이미지 저장
# =============================================================================
# - 포맷: PNG (200 DPI 고해상도)
# - 저장 위치: 워크스페이스 홈 디렉토리
# - 파일 브라우저에서 다운로드 가능
# =============================================================================

output_path = "/Workspace/Users/haesung.cho@bespinglobal.com/dev_haesung_default_ERD.png"

fig.savefig(output_path, dpi=200, bbox_inches='tight', facecolor='white', edgecolor='none')
print(f"ERD saved to: {output_path}")