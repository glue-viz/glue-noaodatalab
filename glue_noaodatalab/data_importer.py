from glue.core import Data
from glue.config import data_importer


@data_importer('NOAO Data Lab')
def noao_importer():
    return Data(label='NOAO')
