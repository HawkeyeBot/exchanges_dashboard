import threading

from sqlalchemy.orm import sessionmaker


class LockableSession:
    def __init__(self, engine):
        self.lock = threading.RLock()
        self.session_maker = sessionmaker()
        self.session_maker.configure(bind=engine)
        self.session = None

    def __enter__(self):
        self.lock.acquire()
        self.session = self.session_maker()
        return self.session

    def __exit__(self, type, value, traceback):
        try:
            self.session.close()
        except:
            pass
        finally:
            self.session = None
        self.lock.release()
