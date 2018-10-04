from .orm import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

_verbose = False
_internal_session = None
_engine = None

def initialize(db_uri, timeout=30, verbose=False):
    global _verbose, _internal_session, _engine, Session

    if '//' not in db_uri:
        db_uri = 'sqlite:///' + db_uri

    _engine = create_engine(db_uri, echo=verbose or _verbose,
                            isolation_level='READ UNCOMMITTED', connect_args={'timeout': timeout})

    Session = sessionmaker(bind=_engine)
    _internal_session=Session()
    Base.metadata.create_all(_engine)

def get_session():
    return _internal_session