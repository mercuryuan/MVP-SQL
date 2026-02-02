from pathlib import Path

DEFAULT_DATA_ROOT = (Path(__file__).parent.parent / "data").resolve()
# 项目所在根目录
PROJECT_ROOT = (Path(__file__).parent.parent).resolve()
# 输出目录
OUTPUT_ROOT = (Path(__file__).parent.parent / "output").resolve()

BIRD_TRAIN_DATABASES_PATH = "E:/BIRD_train/train/train_databases/"
BIRD_DEV_DATABASES_PATH = "E:/BIRD/dev_20240627/dev_databases/"
BIRD_DEV_JSON = "E:/BIRD/dev_20240627/dev.json"  # BIRD所有开发集
BIRD_TRAIN_JSON = "E:/BIRD_train/train/train.json"  # BIRD所有训练集

SPIDER = "All the spider databases"  # 所有spider数据集
SPIDER_TRAIN = "your combination of train and other"  # 所有训练集
SPIDER_DEV_JSON = "E:/spider/dev.json"
SPIDER_DATABASES_PATH = "E:/spider/test_database/"
SPIDER_TRAIN_JSON = "E:/spider/train_spider.json"
SPIDER_TRAIN_OTHER_JSON = "E:/spider/train_others.json"



if __name__ == '__main__':
    print(DEFAULT_DATA_ROOT)
    print(PROJECT_ROOT)
    print(OUTPUT_ROOT)
