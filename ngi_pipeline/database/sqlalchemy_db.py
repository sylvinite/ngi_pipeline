"""sqlalchemy interface to a (currently) sqlite database for tracking running processes"""

import os

from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


LOG = minimal_logger(__name__)

# Declare the base class
Base = declarative_base()
Session = sessionmaker()



@with_ngi_config
def get_db_session(database_path=None, config=None, config_file_path=None):
    """Return a session connection to the database."""
    if not database_path:
        database_path = config['database']['record_tracking_db_path']
    if not os.path.exists(database_path):
        LOG.info('Creating local job tracking database "{}"'.format(database_path))
        # This needs to be called somehow even if the engine already exists
        # possibly this should be module-level
        engine = create_database_populate_schema(database_path)
    else:
        LOG.debug('Local job tracking database at "{}" already exists; connecting.'.format(database_path))
        engine = _init_engine(location)
    # Bind the Session to the engine
    Session.configure(bind=engine)
    # Instantiate
    return Session()
    


def _init_engine(database_path):
    """Create the engine connection."""
    ## engine = create_engine('sqlite:///{}'.format(database_path))
    return create_engine('sqlite:///:memory:', echo=True)


def create_database_populate_schema(location):
    """Create the database and populate it with the schema."""
    engine = _init_engine(location)
    # Create the tables
    Base.metadata.create_all(engine)
    return engine


### USAGE ###

## would be like this
# session = get_db_session()
# seqrun_db = SeqrunAnalysis(project=project.name, sample=sample.name, ...)
# session.add(seqrun_db)
# session.commit()

## later we can query
# session = get_db_session()

# seqrun_processes = session.query(SeqrunAnalysis).filter_by(project=project.name, ...)

#def track_flowcellrun_process(project_id, sample_id, libprep_id,
#                              seqrun_id, engine, process_id):
#    session = get_db_session()
#    FlowcellRunAnalysis(project_id, sample_id, libprep_id, seqrun_id, engine, process_id)


#def track_samplerun_analysis(project, sample, engine, process_id):
#    session = get_db_session()
#    SampleRunAnalysis(project, sample, engine, process_id)


class SeqrunAnalysis(Base):
    __tablename__ = 'seqrunanalysis'

    project_id = Column(String(50))
    sample_id = Column(String(50))
    libprep_id = Column(String(50))
    seqrun_id = Column(String(100))
    # I suppose one day we might have 16 lanes
    lane_num = Column(Integer)
    engine = Column(String(50))
    # We can't use the process id because some processes will not be tracked this way.
    # --> Let's say this is just for Piper at the moment.
    process_id = Column(Integer, primary_key=True)


    def __repr__(self):
        # locals() as a dict for str.format: nice-looking and easy but seems a little sneaky. Discuss!
        return ("<FlowcellRunAnalysis({project_id}/{sample_id}/{libprep_id}/{seqrun_id}/{lane_num}: "
                "process id {process_id}, engine {engine})>".format(project_id=self.project_id,
                                                                    sample_id=self.sample_id,
                                                                    libprep_id=self.libprep_id,
                                                                    seqrun_id=self.seqrun_id,
                                                                    lane_num=self.lane_num,
                                                                    process_id=self.process_id,
                                                                    engine=self.engine))


class SampleAnalysis(Base):
    __tablename__ = 'sampleanalysis'

    project_id = Column(String(50))
    sample_id = Column(String(50), primary_key=True)
    engine = Column(String(50))
    process_id = Column(Integer, unique=True)
    ## Could introduce a ForeignKey to seqrun analyses here
    #seqruns = relationship("SeqrunAnalysis", order_by="SeqrunAnalysis.process_id", backref="sampleanalysis")

    def __repr__(self):
        return ("<SampleRunAnalysis({project_id}/{sample_id}: process id "
                "{process_id}, engine {engine})>".format(project_id=self.project_id,
                                                         sample_id=self.sample_id,
                                                         process_id=self.process_id,
                                                         engine=self.engine))


