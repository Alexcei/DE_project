import os
from utils import to_log


path = '../'
catalog = os.listdir(path)
res = []

for file in catalog:
    if file.split('_')[0] == 'passport':
        res.append(file)

for i in sorted(res):
    postfix = i.split('_')[-1]
    blacklist = 'passport_blacklist_1' + postfix
    terminals = 'terminals_' + postfix
    transactions = 'transactions_' + postfix
    if blacklist not in catalog:
        to_log('', error=f'Not file {blacklist}')
        return

