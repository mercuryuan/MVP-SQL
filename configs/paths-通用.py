from pathlib import Path

DEFAULT_DATA_ROOT = (Path(__file__).parent.parent / "data").resolve()
# 项目所在根目录
PROJECT_ROOT = (Path(__file__).parent.parent).resolve()
# 输出目录
OUTPUT_ROOT = (Path(__file__).parent.parent / "output").resolve()

TRAIN_BIRD = r"E:\BIRD_train\train\train_databases"
SPIDER_DATABASES_PATH = "E:/spider/test_database/"



if __name__ == '__main__':
    print(DEFAULT_DATA_ROOT)
    print(PROJECT_ROOT)
    print(OUTPUT_ROOT)
