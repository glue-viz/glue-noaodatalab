from glue.config import importer


@importer('Import from NOAO Data Lab')
def noao_importer():
    from .data_object import NOAOSQLData
    return [NOAOSQLData('ls_dr6.tractor')]
