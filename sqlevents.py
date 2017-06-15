import os.path as osp
import sqlalchemy as sa
from sqlalchemy import Table, Column, Boolean, Integer, BigInteger, Numeric, String
import pandas as pd


# Column definitions. These will be used later when actually creating tables
# (defined here to avoid repetition later)
COLUMNS = {
    # Columns used in all experiments
    # FIXME: stim params
    "common": [
        Column('id', Integer, primary_key=True),
        Column('subject', String(32), nullable=False, index=True),
        Column('montage', Numeric(2, 1, asdecimal=False), nullable=False),
        Column('experiment', String(32), nullable=False, index=True),
        Column('session', Integer, nullable=False, index=True),
        Column('type', String(32), nullable=False, index=True),
        Column('mstime', BigInteger, nullable=False),
        Column('eegoffset', BigInteger, nullable=False),
        Column('eegfile', String(128), nullable=False),
        Column('exp_version', String(32)),
    ],

    # FR/catFR-specific columns
    "FR": [
        Column('list', Integer),
        Column('serialpos', Integer),
        Column('item_name', String),
        Column('item_num', String),
        Column('recalled', Integer),
        Column('rectime', Integer),
        Column('intrusion', Integer),
        Column('stim_list', Integer),
        Column('is_stim', Integer)
    ],

    # catFR-specific columns
    "catFR": [
        Column('category', String),
        Column('category_num', Integer)
    ]
}


class EventsDatabase(object):
    """SQL representation of events for a specific experiment.

    This supports combining multiple sessions of the same experiment type but
    will fail if trying to combine incompatible experiments.

    Parameters
    ----------
    exp_type : str
        Experiment type (e.g., ``'FR1'``)
    engine : sa.Engine
        SQLAlchemy engine object

    Keyword arguments
    -----------------
    meta : sa.MetaData
    debug : bool

    """
    __allowed_experiments = ['FR1']

    def __init__(self, exp_type, engine, meta=None, debug=False):
        assert exp_type in self.__allowed_experiments
        self.exp_type = exp_type
        self.engine = engine
        self.meta = meta or sa.MetaData()
        self._debug = debug

        self._tables = {
            "events": self._make_events_table()
        }

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        pass

    @property
    def events(self):
        """Return a SQLAlchemy :class:`Table` of events for querying."""
        return self._tables['events']

    def _make_events_table(self):
        if self.exp_type.startswith("FR"):
            columns = COLUMNS['common'] + COLUMNS['FR']

        try:
            return Table("events", self.meta, *columns)
        except sa.exc.ArgumentError:  # table already exists
            return Table("events", self.meta, autoload=True, autoload_with=self.engine)

    def create(self):
        """Generate the tables if they don't already exist."""
        self.meta.create_all(bind=self.engine)

    def from_json(self, path):
        """Convert from JSON events to SQL events.

        Parameters
        ----------
        path : str
            Path to JSON event file to convert.

        """
        columns = [c.name for c in self.events.columns if c.name != 'id']
        df = pd.read_json(path)[columns]
        assert df.experiment[0] == self.exp_type
        if self._debug:
            for n in df.index:
                try:
                    df[df.index == n].to_sql('events', self.engine, if_exists='append', index=False)
                except:
                    print(n)
                    raise
        else:
            df.to_sql('events', self.engine, if_exists='append', index=False)


if __name__ == "__main__":
    # Create empty table with SQLAlchemy
    engine = sa.create_engine('sqlite:///fr1.sqlite')

    with EventsDatabase('FR1', engine) as db:
        db.create()

        # Convert existing JSON events to SQL
        json_file = osp.expanduser('~/mnt/rhino/protocols/r1/subjects/R1297T/experiments/FR1/sessions/0/behavioral/current_processed/all_events.json')
        db.from_json(json_file)

    print('Easy mode: read events directly with pandas')
    df = pd.read_sql_table('events', engine)
    print(df.head())

    print('Restrict read to specific events with pandas')
    df = pd.read_sql_query('select * from events where list = 1', engine)
    print(df.head())

    print('Hard mode: query SQLAlchemy-style')
    with EventsDatabase('FR1', engine) as db:
        with engine.connect() as conn:
            s = sa.select([db.events]).where(db.events.c.list == 1)
            result = conn.execute(s)
            for row in result:
                print(row)
