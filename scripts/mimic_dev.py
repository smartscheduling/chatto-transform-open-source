from chatto_transform.lib.mimic import session

session.login()
session.enable_local_storage('/Users/dan/dev/data/mimic')

import time
start = time.time()

#from chatto_transform.transforms.mimic.age_transform import AgeTransform, AgeHistTransform

# age_df = AgeTransform().load_transform()
# age_count_df = AgeCountTransform().transform(age_df)

from chatto_transform.transforms.mimic.bun_transform import BUNTransform

bun_df = BUNTransform().load_transform()

end = time.time()
print('took', end - start, 'seconds')
