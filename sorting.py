import sys
import mapreduce


class SortingMap(mapreduce.Map):
    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit('1', w)


class SortingReduce(mapreduce.Reduce):
    def reduce(self, k, vlist):
        vlist.sort()
        for v in vlist:
            self.emit(v)


if __name__ == '__main__':

    f = open(sys.argv[1])
    values = f.readlines()
    f.close()

    engine = mapreduce.Engine(values, SortingMap, SortingReduce)
    engine.execute()
    result_list = engine.get_result_list()

    for r in result_list:
        print r
