from cgc_api import db


class Token(db.Model):
    __tablename__ = 'T_CGC_API_TOKEN'

    TokenID = db.Column(db.Integer, primary_key=True)
    TokenHash = db.Column(db.String, nullable=False)
    MachineID = db.Column(db.Integer,
                          db.ForeignKey('T_CGC_API_MACHINE.MachineID'),
                          nullable=False)
    OperatorID = db.Column(db.Integer,
                           db.ForeignKey('T_CGC_API_OPERATOR.OperatorID'),
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
    OperatorCode = db.Column(db.Integer, unique=True, nullable=False)
    Password = db.Column(db.String, nullable=False)
    Active = db.Column(db.Boolean, nullable=False)

    def __repr__(self):
        return '<Operator %r>' % self.OperatorCode
