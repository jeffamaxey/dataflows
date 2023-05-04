import os
import itertools
from dataflows import Flow
from .stream import stream
from .unstream import unstream


def _notify_checkpoint_saved(checkpoint_name):

    def step(package):
        yield package.pkg
        for rows in package:
            yield iter(rows)
        print(f'checkpoint saved: {checkpoint_name}')

    return step


class checkpoint(Flow):

    def __init__(self, checkpoint_name, checkpoint_path='.checkpoints', steps=None, resources=None):
        if not steps:
            steps = []
        super().__init__(*steps)
        self.checkpoint_name = checkpoint_name
        self.checkpoint_path = os.path.join(checkpoint_path, checkpoint_name)
        self.resources = resources

    @property
    def filename(self):
        return os.path.join(self.checkpoint_path, 'stream.ndjson')

    def exists(self):
        return os.path.exists(self.filename)

    def _preprocess_chain(self):
        if os.path.exists(self.filename):
            print(f'using checkpoint data from {self.checkpoint_path}')
            return unstream(self.filename),
        else:
            print(f'saving checkpoint to: {self.checkpoint_path}')
            return itertools.chain(self.chain, (stream(self.filename),
                                                _notify_checkpoint_saved(self.checkpoint_name)))

    def handle_flow_checkpoint(self, parent_chain):
        self.chain = itertools.chain(self.chain, parent_chain)
        return [self]
