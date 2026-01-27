from pathlib import Path

DEFAULT_DATA_ROOT = (Path(__file__).parent.parent / "data").resolve()
# 项目所在根目录
PROJECT_ROOT = (Path(__file__).parent.parent).resolve()
# 输出目录
OUTPUT_ROOT = (Path(__file__).parent.parent / "output").resolve()

TRAIN_BIRD = r"F:\train_bird\train_databases"



if __name__ == '__main__':
    print(DEFAULT_DATA_ROOT)
    print(PROJECT_ROOT)
    print(OUTPUT_ROOT)
