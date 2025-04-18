class DummyThread:
    def __init__(self, *, target, daemon, name):
        self.target = target
        self.daemon = daemon
        self.name = name
        self.started = False

    def start(self):
        self.started = True

    def join(self, timeout=None):
        pass