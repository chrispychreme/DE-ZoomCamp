import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    year_month = ["2020-10","2020-11","2020-12"]

    output = pd.DataFrame()
    
    for x in year_month:
        df = pd.read_parquet(path=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{x}.parquet")
        output = pd.concat([output, df])
    
    return output


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
