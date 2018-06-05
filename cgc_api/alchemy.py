from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
import inspect

Base = automap_base()

# engine, suppose it has two tables 'user' and 'address' set up
engine = create_engine("mssql+pymssql://sqladmin:AcChRgHax2C0p3s@10.7.2.1/AGENCE_TAROUDANT?charset=utf8")

# reflect the tables
Base.prepare(engine, reflect=True)

print(Base.classes)
print(Base.classes['T_AGENCE'])

agence = Base.classes['T_AGENCE']
attrs = inspect.getmembers(agence, lambda a:not(inspect.isroutine(a)))
print([a[0] for a in attrs if not(a[0].startswith('__') and a[0].endswith('__'))])

setattr(agence, 'ADRESSE', 'Test')
print(agence.ADRESSE)
#session = sessionmaker(bind=engine)

#s = session()

#print(s.query(agence).all())
