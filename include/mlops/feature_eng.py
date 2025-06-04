import polars as pl
from sklearn.feature_extraction import DictVectorizer


def fit_transform_dv(df: pl.DataFrame, dv: DictVectorizer = None, fit_dv: bool = False):
    feature_mapping = df.to_dicts()
    dv = DictVectorizer(sparse=True)
    X = dv.fit_transform(feature_mapping)
    return X, dv


def transform_dv(df: pl.DataFrame, dv: DictVectorizer):
    feature_mapping = df.to_dicts()
    return dv.transform(feature_mapping)
