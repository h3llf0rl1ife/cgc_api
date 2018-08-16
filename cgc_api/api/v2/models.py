from cgc_api import db

"""
class Journee(db.Model):
    __tablename__ = 'T_JOURNEE'

    date_journee = db.Column('DATE_JOURNEE', db.Date, primary_key=True)
    stock = db.Column('STOCK', db.Boolean, nullable=True)
    cloture = db.Column('CLOTURE', db.Boolean, nullable=True)
    solde_emb = db.Column('SOLDE_EMB', db.Boolean, nullable=True)
    solde_caisse = db.Column('SOLDE_CAISSE', db.Boolean, nullable=True)
    solde_caisserie = db.Column('SOLDE_CAISSERIE', db.Boolean, nullable=True)
    code_agce = db.Column('CODE_AGCE', db.Integer, nullable=False)
    temp = db.Column('JOURNEE_TEMP', db.Boolean, nullable=True)

    as_c_std = db.Column('as_c_std', db.Integer, nullable=True)
    as_p_ag = db.Column('as_p_ag', db.Integer, nullable=True)
    as_p_uht = db.Column('as_p_uht', db.Integer, nullable=True)
    as_c_ag = db.Column('as_c_ag', db.Integer, nullable=True)
    as_c_pr = db.Column('as_c_pr', db.Integer, nullable=True)
    ns_c_std = db.Column('ns_c_std', db.Integer, nullable=True)
    ns_p_ag = db.Column('ns_p_ag', db.Integer, nullable=True)
    ns_p_uht = db.Column('ns_p_uht', db.Integer, nullable=True)
    ns_c_ag = db.Column('ns_c_ag', db.Integer, nullable=True)
    ns_c_pr = db.Column('ns_c_pr', db.Integer, nullable=True)
    as_p_euro = db.Column('as_p_euro', db.Integer, nullable=True)
    as_cs_blc = db.Column('as_cs_blc', db.Integer, nullable=True)
    ns_pal_euro = db.Column('ns_pal_euro', db.Integer, nullable=True)
    ns_cs_blc = db.Column('ns_cs_blc', db.Integer, nullable=True)
    as_cs1 = db.Column('as_cs1', db.Integer, nullable=True)
    as_cs2 = db.Column('as_cs2', db.Integer, nullable=True)
    nv_cs1 = db.Column('nv_cs1', db.Integer, nullable=True)
    nv_cs2 = db.Column('nv_cs2', db.Integer, nullable=True)

    def __repr__(self):
        return '<Journée %r>' % self.date_journee


class StockInit(db.Model):
    __tablename__ = 'T_STOCK_INIT'

    date_ps = db.Column('date_ps', db.Date, primary_key=True)
    code_article = db.Column('code_article', db.Integer, primary_key=True)
    code_magasin = db.Column('code_magasin', db.Integer, primary_key=True)
    categorie = db.Column('categorie', db.String(15), primary_key=True)
    qte_init = db.Column('qte_init', db.Float, nullable=True)
    code_agce = db.Column('code_agce', db.Integer, nullable=True)


class StockInitCond(db.Model):
    __tablename__ = 'T_STOCK_INIT_COND'

    date_journee = db.Column('date_journee', db.Date, primary_key=True)
    code_cp = db.Column('code_cp', db.Integer, primary_key=True)
    code_magasin = db.Column('code_magasin', db.Integer, primary_key=True)
    stock_init = db.Column('stock_init', db.Integer, nullable=True)
    code_agce = db.Column('code_agce', db.Integer, nullable=True)


class ArticlesMagasins(db.Model):
    __tablename__ = 'T_ARTICLES_MAGASINS'

    code_article = db.Column('code_article', db.Integer, primary_key=True)
    categorie = db.Column('categorie', db.String(15), primary_key=True)
    magasin = db.Column('magasin', db.Integer, primary_key=True)
    qte_stock = db.Column('qte_stock', db.Integer, nullable=True)


class MagasinCond(db.Model):
    __tablename__ = 'T_MAGASIN_COND'

    code_magasin = db.Column('code_magasin', db.Integer, primary_key=True)
    code_cp = db.Column('code_cp', db.Integer, primary_key=True)
    qte_stock = db.Column('qte_stock', db.Integer, nullable=True)


class PreparationChargements(db.Model):
    __tablename__ = 'T_PREPARATION_CHARGEMENTS'

    code_article = db.Column('code_article', db.Integer, primary_key=True)
    importe = db.Column('importe', db.Integer, nullable=True)
    sortie1 = db.Column('sortie1', db.Integer, nullable=True)
    sortie2 = db.Column('sortie2', db.Integer, nullable=True)
    charg_gms = db.Column('charg_gms', db.Integer, nullable=True)
    rest_stock = db.Column('rest_stock', db.Integer, nullable=True)
    code_agce = db.Column('code_agce', db.Integer, primary_key=True)


class Repartition(db.Model):
    __tablename__ = 'T_REPARTITION'

    date_repartition = db.Column('date_repartition', db.Date, primary_key=True)
    code_agence = db.Column('code_agence', db.Integer, primary_key=True)
    livraisons = db.Column('livraisons', db.Boolean, nullable=True)
    validation = db.Column('validation', db.Boolean, nullable=True)
    code_operateur = db.Column('code_operateur', db.Integer, nullable=True)
    controleur_produit = db.Column(
        'controleur_produit', db.Integer, nullable=True)
    controleur_cond = db.Column('controleur_cond', db.Integer, nullable=True)


class Prix(db.Model):
    __tablename__ = 'T_PRIX'

    date_debut = db.Column('date_debut', db.Date, primary_key=True)
    date_fin = db.Column('date_fin', db.Date, nullable=True)
    code_agce = db.Column('code_agce', db.Integer, primary_key=True)
    code_article = db.Column('code_article', db.Integer, primary_key=True)
    prix = db.Column('prix', db.Float, nullable=True)
    prix_ht = db.Column('prix_ht', db.Float, nullable=True)


class PrixAgence(db.Model):
    __tablename__ = 'T_PRIX_AGENCE'

    code_agce = db.Column('code_agce', db.Integer, primary_key=True)
    code_article = db.Column('code_article', db.Integer, primary_key=True)
    prix_vente = db.Column('prix_vente', db.Float, nullable=True)
    taux_commission = db.Column('taux_commission', db.Float, nullable=True)


class OperationsCaisse(db.Model):
    __tablename__ = 'T_OPERATIONS_CAISSE'

    code_operation = db.Column('code_operation', db.Integer, primary_key=True)
    date_validation = db.Column('date_validation', db.Date, nullable=True)
    montant = db.Column('montant', db.Float, nullable=True)
    type_operation = db.Column('type_operation', db.String(1), nullable=True)
    code_agce = db.Column('code_agce', db.Integer, nullable=True)


class SoldeInitialCaisse(db.Model):
    __tablename__ = 'T_SOLDE_INITIAL_CAISSE'

    date_journee = db.Column('date_journee', db.Date, primary_key=True)
    code_caisse = db.Column('code_caisse', db.Integer, primary_key=True)
    solde_initial = db.Column('solde_initial', db.Float, nullable=True)


class LivraisonPlanning(db.Model):
    __tablename__ = 'T_LIVRAISON_PLANNING'

    code_client = db.Column('code_client', db.Integer, primary_key=True)
    code_agce = db.Column('code_agce', db.Integer, nullable=True)
    derniere_maj = db.Column('dernier_maj', db.Date, nullable=True)
    lundi = db.Column('lundi', db.Boolean, nullable=True)
    mardi = db.Column('mardi', db.Boolean, nullable=True)
    mercredi = db.Column('mercredi', db.Boolean, nullable=True)
    jeudi = db.Column('jeudi', db.Boolean, nullable=True)
    vendredi = db.Column('vendredi', db.Boolean, nullable=True)
    samedi = db.Column('samedi', db.Boolean, nullable=True)
    dimanche = db.Column('dimanche', db.Boolean, nullable=True)


class HistoriqueOperations(db.Model):
    __tablename__ = 'T_HISTORIQUE_OPERATIONS'

    id_evenement = db.Column('id_evenement', db.Integer, primary_key=True)
    date_heure = db.Column('date_heure', db.DateTime, nullable=False)
    commentaire = db.Column('commentaire', db.String(100), nullable=True)
    cat = db.Column('cat', db.String(3), nullable=True)
    code_operateur = db.Column('code_operateur', db.Integer, nullable=True)
    poste = db.Column('poste', db.String(15), nullable=True)
    session = db.Column('session', db.String(15), nullable=True)


class SyntheseLivraision(db.Model):
    __bind_key__ = 'stats'
    __tablename__ = 'T_SYNTHESE_LIVRAISON'

    date_journee = db.Column('date_journee', db.Date, primary_key=True)
    code_client = db.Column('code_client', db.Integer, primary_key=True)
    code_agce = db.Column('code_agce', db.Integer, nullable=True)
    programme = db.Column('programme', db.Boolean, nullable=True)
    commande = db.Column('commande', db.Boolean, nullable=True)
    livre = db.Column('livre', db.Boolean, nullable=True)


class MoyVenteArticle(db.Model):
    __tablename__ = 'T_MOY_VENTE_ARTICLE'

    date_vente = db.Column('date_vente', db.Date, primary_key=True)
    code_produit = db.Column('code_produit', db.Integer, primary_key=True)
    code_secteur = db.Column('code_secteur', db.Integer, primary_key=True)
    qte_vente = db.Column('qte_vente', db.Float, nullable=True)
    qte_perte = db.Column('qte_perte', db.Float, nullable=True)
    qte_invendu = db.Column('qte_invendu', db.Float, nullable=True)


class MoyVenteClients(db.Model):
    __tablename__ = 'T_MOY_VENTE_CLIENTS'

    date_vente = db.Column('date_vente', db.Date, primary_key=True)
    code_client = db.Column('code_client', db.Integer, primary_key=True)
    code_produit = db.Column('code_produit', db.Integer, primary_key=True)
    qte_vente = db.Column('qte_vente', db.Float, nullable=True)
    qte_perte = db.Column('qte_perte', db.Float, nullable=True)


class Clients(db.Model):
    __tablename__ = 'T_CLIENTS'

    code_client = db.Column('code_client', db.Integer, primary_key=True)
    code_sous_secteur = db.Column(
        'code_sous_secteur', db.Integer,
        db.ForeignKey('T_SECTEUR.CODE_SOUS_SECTEUR'), nullable=True)
    
    sous_secteurs = db.relationship(
        'SousSecteur', backref='clients', lazy=True)


class SousSecteur(db.Model):
    __tablename__ = 'T_SOUS_SECTEUR'

    code_sous_secteur = db.Column(
        'code_sous_secteur', db.Integer, primary_key=True)
    code_secteur = db.Column(
        'code_secteur', db.Integer,
        db.ForeignKey('T_SECTEUR.CODE_SECTEUR'), nullable=False)

    secteur = db.relationship(
        'Secteur', backref='sousSecteurs', lazy=True)


class Secteur(db.Model):
    __tablename__ = 'T_SECTEUR'

    code_secteur = db.Column('code_secteur', db.Integer, primary_key=True)
    code_bloc = db.Column(
        'code_bloc', db.Integer,
        db.ForeignKey('T_BLOC.CODE_BLOC'), nullable=False)

    bloc = db.relationship(
        'Bloc', backref='secteurs', lazy=True)


class Bloc(db.Model):
    __tablename__ = 'T_BLOC'

    code_bloc = db.Column('code_bloc', db.Integer, primary_key=True)
    code_zone = db.Column(
        'code_zone', db.Integer,
        db.ForeignKey('T_ZONE.CODE_ZONE'), nullable=False)

    zone = db.relationship(
        'Zone', backref='blocs', lazy=True)


class Zone(db.Model):
    __tablename__ = 'T_ZONE'

    code_zone = db.Column('code_zone', db.Integer, primary_key=True)
    code_agce = db.Column(
        'code_agce', db.Integer,
        db.ForeignKey('T_AGENCE.CODE_AGCE'), nullable=False)

    agence = db.relationship(
        'Agence', backref='zones', lazy=True)


class Agence(db.Model):
    __tablename__ = 'T_AGENCE'

    code_agce = db.Column('code_agce', db.Integer, primary_key=True)
"""