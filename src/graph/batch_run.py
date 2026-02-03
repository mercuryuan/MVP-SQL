import os
import time
import logging
from pathlib import Path
from tqdm import tqdm
import sys

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 引入你的新模块
from pipeline import SchemaPipeline
from configs import paths

# 配置日志
logging.basicConfig(
    filename="pipeline_batch_run.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding='utf-8'
)


def process_dataset(dataset_name, dataset_root_path, skip_existing=False):
    """
    批量处理指定数据集下的所有数据库。

    :param dataset_name: 数据集名称 (e.g., 'bird', 'spider')，用于生成输出目录层级
    :param dataset_root_path: 数据集根目录 (包含各个数据库文件夹的目录)
    :param skip_existing: 如果目标 pkl 文件已存在，是否跳过
    """
    root_dir = Path(dataset_root_path)

    if not root_dir.exists():
        print(f"❌ 错误: 数据集路径不存在: {root_dir}")
        return

    # 1. 扫描所有子目录，寻找 .sqlite 文件
    # 假设结构: root / db_name / db_name.sqlite
    db_dirs = [d for d in root_dir.iterdir() if d.is_dir()]

    print(f"\n🚀 开始处理数据集: [{dataset_name}]")
    print(f"📂 扫描到 {len(db_dirs)} 个数据库文件夹")
    print(f"📂 输出根目录: {paths.OUTPUT_ROOT}")

    # 打印跳过模式状态
    if skip_existing:
        print("⏩ 已开启断点续传模式：检测到目标文件存在将自动跳过")

    success_count = 0
    fail_count = 0
    skip_count = 0  # 新增统计

    # 使用 tqdm 显示进度
    pbar = tqdm(db_dirs, desc=f"Building Graphs ({dataset_name})", unit="db")

    for db_dir in pbar:
        db_name = db_dir.name

        # 寻找该目录下的 sqlite 文件
        sqlite_files = list(db_dir.glob("*.sqlite"))

        if not sqlite_files:
            logging.warning(f"Skipping {db_name}: No .sqlite file found.")
            continue

        # 默认取第一个 sqlite 文件
        sqlite_path = sqlite_files[0]

        # 2. 构建输出路径
        # 结构: output / dataset / db_name / db_name.pkl
        output_dir = paths.OUTPUT_ROOT / dataset_name / db_name
        output_pkl = output_dir / f"{db_name}.pkl"

        # === 核心修改：检测存在则跳过 ===
        if skip_existing and output_pkl.exists():
            skip_count += 1
            # 记录日志，并在进度条后缀显示状态，但不打印刷屏
            logging.info(f"Skipping {db_name}: Output file already exists -> {output_pkl}")
            pbar.set_postfix(status="Skipped", db=db_name)
            continue
        # ============================

        # 3. 执行 Pipeline
        try:
            # 确保输出目录存在
            output_dir.mkdir(parents=True, exist_ok=True)

            # 更新进度条描述
            pbar.set_postfix(status="Processing", db=db_name)

            # 这里的 SchemaPipeline 封装了所有细节：SQLite读取 -> 分析 -> 构建图 -> 保存
            pipeline = SchemaPipeline(str(sqlite_path), str(output_pkl))
            pipeline.run()  # 内部已经包含了 tqdm (列级别)

            success_count += 1
            logging.info(f"Success: {db_name} -> {output_pkl}")

        except Exception as e:
            fail_count += 1
            error_msg = f"Failed: {db_name}. Error: {str(e)}"
            logging.error(error_msg)

    print(f"\n✅ [{dataset_name}] 处理完成 Summary:")
    print(f"   - 成功: {success_count}")
    print(f"   - 失败: {fail_count}")
    print(f"   - 跳过: {skip_count}")  # 输出跳过数量
    print(f"   - 日志已保存至 pipeline_batch_run.log")


if __name__ == "__main__":
    # ================= 配置区域 =================

    # 1. BIRD 数据集配置
    # 注意：这里使用 getattr 是为了防止 paths.py 中没有定义 TRAIN_BIRD 导致报错，默认回退到你之前的路径
    bird_path = getattr(paths, "TRAIN_BIRD", r"F:\train_bird\train_databases")

    # 2. SPIDER 数据集配置
    # spider_path = getattr(paths, "SPIDER_TRAIN", r"../data/spider/database")

    # ================= 执行区域 =================

    # 执行 BIRD
    if os.path.exists(bird_path):
        process_dataset(
            dataset_name="bird",
            dataset_root_path=bird_path,
            skip_existing=True  # 【修改】开启跳过模式，避免覆盖生成
        )
    else:
        print(f"❌ 未找到 BIRD 数据集路径: {bird_path}")

    # 执行 SPIDER (稍后配置好路径后取消注释)
    # if os.path.exists(spider_path):
    #     process_dataset("spider", spider_path, skip_existing=True)
