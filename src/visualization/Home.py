import streamlit as st
import sys
from pathlib import Path

# Add project root to Python path

# This file is at src/visualization/Home.py
# Root is at ../../
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

st.set_page_config(
    page_title="MVP-SQL Visualization Hub",
    page_icon="🔍",
    layout="wide"
)

st.title("MVP-SQL Visualization Hub 🚀")

st.markdown("""
## 欢迎来到MVP-SQL可视化系统

该应用集成了两种强大的数据库模式分析和SQL查询可视化工具。

### 👈 从侧边栏选择一个模块：

1. **模式查看器**：探索数据库模式图结构，包括表、列和关系。
2. **SQL 分析器**：分析并可视化 SQL 查询、其执行计划及子图提取。

---

**项目结构：**
- 'src/visualization/pages/1_Schema_Viewer.py'：交互式图可视化
- 'src/visualization/pages/2_SQL_Analyzer.py'：SQL 解析与子图分析
""")
# streamlit run src/visualization/Home.py
