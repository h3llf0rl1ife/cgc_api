from cgc_api import db


class Token(db.Model):
    __tablename__ = 'T_CGC_API_TOKEN'

    TokenID = db.Column(db.Integer, primary_key=True)
    TokenHash = db.Column(db.String, nullable=False)
    MachineID = db.Column(
        db.Integer, db.ForeignKey('T_CGC_API_MACHINE.MachineID'),
        nullable=False)
    OperatorID = db.Column(
        db.Integer, db.ForeignKey('T_CGC_API_OPERATOR.OperatorID'),
        nullable=False)
    IssuedAt = db.Column(db.DateTime, nullable=False)
    ExpiresAt = db.Column(db.DateTime, nullable=False)
    Active = db.Column(db.Boolean, nullable=False)

    Machine = db.relationship('Machine', backref='token', lazy=True)
    Operator = db.relationship('Operator', backref='token', lazy=True)

    def __repr__(self):
        return '<Token %r>' % self.TokenHash


class Machine(db.Model):
    __tablename__ = 'T_CGC_API_MACHINE'

    MachineID = db.Column(db.Integer, primary_key=True)
    MachineName = db.Column(db.String(120), unique=True, nullable=False)
    Active = db.Column(db.Boolean, nullable=False)

    def __repr__(self):
        return '<Machine %r>' % self.MachineName


class Operator(db.Model):
    __tablename__ = 'T_CGC_API_OPERATOR'

    OperatorID = db.Column(db.Integer, primary_key=True)
    OperatorCode = db.Column(
        db.Integer,
        db.ForeignKey('T_OPERATEUR.CODE_OPERATEUR'),
        nullable=False)
    Password = db.Column(db.String, nullable=False)
    Salt = db.Column(db.String, nullable=False)

    Operateur_ = db.relationship(
        'Operateur', backref='Operator_', lazy=True, uselist=False)

    def __repr__(self):
        return '<Operator %r>' % self.OperatorCode


class Operateur(db.Model):
    __tablename__ = 'T_OPERATEUR'

    OperatorCode = db.Column('CODE_OPERATEUR', db.Integer, primary_key=True)
    OperatorName = db.Column('NOM_OPERATEUR', db.String(30), nullable=True)
    AgencyCode = db.Column('CODE_AGCE', db.Integer, nullable=True)
    Password = db.Column('MDP', db.String(30), nullable=True)
    Active = db.Column('ACTIF', db.Boolean, nullable=False)
    Function = db.Column('FONCTION', db.Integer, nullable=True)
    SerialNumber = db.Column('MATRICULE', db.Integer, nullable=True)

    def __repr__(self):
        return '<Operateur %r>' % self.OperatorName


class OperateurTache(db.Model):
    __tablename__ = 'T_OPERTEURS_TACHES'

    OperatorCode = db.Column(
        'CODE_OPERATEUR', db.Integer,
        db.ForeignKey('T_OPERATEUR.CODE_OPERATEUR'), nullable=False)
    TaskID = db.Column('ID_TACHE', db.Integer, primary_key=True)
    State = db.Column('ETAT', db.Integer, nullable=True)

    Operateur_ = db.relationship(
        'Operateur', backref='Task_', lazy=True, uselist=False)

    def __repr__(self):
        return '<Task %r>' % self.TaskID
