from .. import DataStreamProcessor


class conditional(DataStreamProcessor):

    def __init__(self, predicate, flow):
        super().__init__()
        self.predicate = predicate
        self.flow = flow

    def _process(self):
        ds = self.source._process()
        if not self.predicate(ds.dp):
            return ds
        flow = self.flow(ds.dp) if callable(self.flow) else self.flow
        return flow.datastream(ds)
