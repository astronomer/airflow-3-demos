import yaml
from sklearn.linear_model import LinearRegression


def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_lin_reg_model(x_train, y_train) -> LinearRegression:
    return LinearRegression().fit(x_train, y_train)
