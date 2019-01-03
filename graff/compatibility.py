import sys
import random

if sys.version_info[0]<3:
    from functools import partial
    class partialmethod(partial):
        def __get__(self, instance, owner):
            if instance is None:
                return self
            return partial(self.func, instance,
                           *(self.args or ()), **(self.keywords or {}))


else:
    from functools import partialmethod


if sys.version_info<(3,6):
    if sys.version_info[0]==3:
        xrange = range

    class Random(random.Random):
        def choices(self, population, k=1):
            return [self.choice(population) for i in xrange(k)]
else:
    Random = random.Random