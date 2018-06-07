import pymssql
import datetime
import decimal

from dateutil.parser import parse


class Queries(object):
    def __init__(self, server, user, password, database):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
    
    
    def executeQuery(self, query):
        if type(query) != str:
            raise query
        
        with pymssql.connect(self.server, self.user, self.password, self.database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                try:
                    cursor.execute(query)
                except pymssql.ProgrammingError:
                    raise

                entries = cursor.fetchall()
                for entry in entries:
                    for cell in entry:
                        if type(entry[cell]) is datetime.datetime:
                            entry[cell] = str(entry[cell])
                        elif type(entry[cell]) is decimal.Decimal:
                            entry[cell] = float(entry[cell])

        return entries    

    @staticmethod
    def validateDate(kwarg, default=None):
        # default: 0 = min date, 1 = now
        try:
            x = parse(kwarg)
            return x.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            dates = (datetime.datetime(1900, 1, 1, 00, 00, 00), datetime.datetime.now())
            return dates[default].strftime('%Y-%m-%d %H:%M:%S')
        return default

    
    def is_op_auth_for_tache(self, args): #Done
        query = '''
            SELECT 
                T_OPERTEURS_TACHES.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERTEURS_TACHES.ID_TACHE AS ID_TACHE,	
                T_OPERTEURS_TACHES.ETAT AS ETAT
            FROM 
                T_OPERTEURS_TACHES
            WHERE 
                {OPTIONAL_ARG_1}
                {OPTIONAL_ARG_2}
        '''

        try:
            kwargs = {
                'pcodeOp': args[0],
                'pCodeTache': args[1]
            }
        except IndexError as e:
            return e

        kwargs['OPTIONAL_ARG_1'] = 'T_OPERTEURS_TACHES.CODE_OPERATEUR = {pcodeOp}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_OPERTEURS_TACHES.ID_TACHE = {pCodeTache}'

        if kwargs['pcodeOp'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
            kwargs['OPTIONAL_ARG_2'] = kwargs['OPTIONAL_ARG_2'][4:]
        
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['pCodeTache'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)
    
    
    def Param_ls_clients_gp(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_CLIENTS.GROUP_CLIENT AS GROUP_CLIENT
            FROM 
                T_CLIENTS
            WHERE 
                T_CLIENTS.ACTIF = 1
                {OPTIONAL_ARG_1}
                {OPTIONAL_ARG_2}
            ORDER BY 
                NOM_CLIENT ASC
        '''

        try:
            kwargs = {
                'Param_gp': args[0],
                'Param_not_in': args[1]
            }
        except IndexError as e:
            return e

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_CLIENTS.GROUP_CLIENT = {Param_gp}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_CLIENTS.CODE_CLIENT NOT IN ({Param_not_in})'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_gp'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_not_in'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)
    
    
    def Param_supp_objectif_secteurs(self, args): #Done
        query = '''
            DELETE FROM 
                T_OBJECTIF_SECTEURS
            WHERE 
                T_OBJECTIF_SECTEURS.DATE_OBJECTIF = '{Param_date_journee}'
                AND	T_OBJECTIF_SECTEURS.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        return query.format(**kwargs)

    
    def Req_affectation_chargement(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_CHARGEMENT.code_chauffeur AS code_chauffeur,	
                T_CHARGEMENT.AIDE_VENDEUR1 AS AIDE_VENDEUR1,	
                T_CHARGEMENT.AIDE_VENDEUR2 AS AIDE_VENDEUR2,	
                T_CHARGEMENT.vehicule AS vehicule,	
                T_CHARGEMENT.VALID AS VALID
            FROM 
                T_CHARGEMENT
            WHERE 
                {OPTIONAL_ARG_1}
                {OPTIONAL_ARG_2}
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'], 0)

        kwargs['OPTIONAL_ARG_1'] = '''T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}' '''
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}'

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
            kwargs['OPTIONAL_ARG_2'] = kwargs['OPTIONAL_ARG_2'][4:]
        
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_anc_solde_dep(self, args): #Done
        query = '''
            SELECT 
                SUM(T_MOUVEMENTS_CAISSE.MONTANT) AS la_somme_MONTANT,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION
            FROM 
                T_OPERATIONS_CAISSE,	
                T_MOUVEMENTS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.CODE_OPERATION = T_MOUVEMENTS_CAISSE.ORIGINE
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_OPERATIONS_CAISSE.DATE_OPERATION < '{Param_date_journee}'
                    AND	
                    (
                        T_OPERATIONS_CAISSE.DATE_VALIDATION = '{Param_date_journee}'
                        OR	T_OPERATIONS_CAISSE.DATE_VALIDATION = '1900-01-01 00:00:00'
                    )
                    AND	T_OPERATIONS_CAISSE.TYPE_OPERATION IN ('D', 'V') 
                )
            GROUP BY 
                T_OPERATIONS_CAISSE.DATE_VALIDATION
        '''
        
        try:
            kwargs = {
                'Param_code_caisse': args[0],
                'Param_date_journee': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'], 0)

        kwargs['OPTIONAL_ARG_1'] = 'T_MOUVEMENTS_CAISSE.CODE_CAISSE = {Param_code_caisse} AND'

        return query.format(**kwargs).format(**kwargs)

    
    def Req_annulation_facture(self, args): #Done
        query = '''
            UPDATE 
                T_FACTURE
            SET
                VALID = 0
            WHERE 
                T_FACTURE.NUM_FACTURE = {Param_num_facture}
        '''

        try:
            kwargs = {
                'Param_num_facture': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_num_facture'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_annulation_gratuit_prevente(self, args): #Done
        query = '''
            UPDATE
                T_LIGNE_COMMANDE
            SET 
                T_LIGNE_COMMANDE.QTE_PROMO = 0
            WHERE 
                ID_COMMANDE IN (SELECT ID_COMMANDE FROM T_COMMANDE_CLIENT WHERE DATE_COMMANDE='{param_date}' AND code_secteur={param_code_secteur})
                AND T_LIGNE_COMMANDE.QTE_LIVREE = 0 AND T_LIGNE_COMMANDE.QTE_COMMANDE>0
        '''

        try:
            kwargs = {
                'param_date': args[0],
                'param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['param_date'] = self.validateDate(kwargs['param_date'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        return query.format(**kwargs)

    
    def Req_annule_synchro(self, args): #Done
        query = '''
            UPDATE 
                T_SYNCHRO
            SET
                ETAT = 2
            WHERE 
                T_SYNCHRO.OPERATION = {Param_operation}
                AND	T_SYNCHRO.SOUS_OPERATION = {Param_sous_operation}
                AND	T_SYNCHRO.ID_OPERATION = {Param_id_operation}
        '''

        try:
            kwargs = {
                'Param_operation': args[0],
                'Param_sous_operation': args[1],
                'Param_id_operation': args[2]
            }
        except IndexError as e:
            return e

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        return query.format(**kwargs)

    
    def Req_article_livraison(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.ORIGINE AS ORIGINE,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_THEORIQUE AS QTE_THEORIQUE,	
                T_MOUVEMENTS.QTE_REEL AS QTE_REEL,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                T_MOUVEMENTS.QTE_CAISSE AS QTE_CAISSE,	
                T_MOUVEMENTS.QTE_PAL AS QTE_PAL
            FROM 
                T_MOUVEMENTS
            WHERE 
                {OPTIONAL_ARG_1}
                {OPTIONAL_ARG_2}
        '''

        try:
            kwargs = {
                'Param_origine': args[0],
                'Param_code_article': args[1]
            }
        except IndexError as e:
            return e

        kwargs['OPTIONAL_ARG_1'] = 'T_MOUVEMENTS.ORIGINE = {Param_origine}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_MOUVEMENTS.CODE_ARTICLE = {Param_code_article}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_origine'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_article'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_article_livree_gms_date(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.STATUT <> 'A'
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
            GROUP BY 
                T_LIVRAISON.DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT,	
                T_PRODUITS_LIVREES.CODE_ARTICLE,	
                T_LIVRAISON.NUM_LIVRAISON,	
                T_LIVRAISON.TYPE_MVT
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_date_livraison': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_LIVRAISON.CODE_CLIENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_2'] = '''AND T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}' '''

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_article_magasins(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES_MAGASINS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES_MAGASINS.MAGASIN AS MAGASIN,	
                T_ARTICLES_MAGASINS.CATEGORIE AS CATEGORIE,	
                T_ARTICLES_MAGASINS.QTE_STOCK AS QTE_STOCK
            FROM 
                T_ARTICLES_MAGASINS
            WHERE 
                T_ARTICLES_MAGASINS.CATEGORIE = 'PRODUIT'
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_code_article': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_ARTICLES_MAGASINS.CODE_ARTICLE = {Param_code_article}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_article'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_articles_charges(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE AS QTE_CHARGEE,	
                T_PRODUITS_CHARGEE.QTE_COND AS QTE_COND,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_VAL AS QTE_CHARGEE_VAL,	
                T_PRODUITS_CHARGEE.MONTANT AS MONTANT,	
                T_PRODUITS_CHARGEE.TOTAL_VENDU AS TOTAL_VENDU,	
                T_PRODUITS_CHARGEE.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.QTE_ECART AS QTE_ECART,	
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_SUPP AS QTE_CHARGEE_SUPP,	
                T_PRODUITS_CHARGEE.TOTAL_CHARGEE AS TOTAL_CHARGEE,	
                T_PRODUITS_CHARGEE.code_vendeur AS code_vendeur,	
                T_PRODUITS_CHARGEE.CMD_U AS CMD_U,	
                T_PRODUITS_CHARGEE.CMD_C AS CMD_C,	
                T_PRODUITS_CHARGEE.CREDIT AS CREDIT,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE AS QTE_CHARGEE_POINTE
            FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                {OPTIONAL_ARG_1}
                {OPTIONAL_ARG_2}
                {OPTIONAL_ARG_3}
        '''

        try:
            kwargs = {
                'Param_code_article': args[0],
                'Param_code_secteur': args[1],
                'Param_date_chargement': args[2]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        kwargs['OPTIONAL_ARG_1'] = 'T_PRODUITS_CHARGEE.CODE_ARTICLE = {Param_code_article}'
        kwargs['OPTIONAL_ARG_2'] = 'AND T_PRODUITS_CHARGEE.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_3'] = 'AND T_PRODUITS_CHARGEE.DATE_CHARGEMENT = {Param_date_chargement}'

        if kwargs['Param_code_article'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
            kwargs['OPTIONAL_ARG_2'] = kwargs['OPTIONAL_ARG_2'][4:]

        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_3'] = ''

        elif kwargs['OPTIONAL_ARG_1'] == '' and kwargs['OPTIONAL_ARG_2'] == '':
            kwargs['OPTIONAL_ARG_3'] = kwargs['OPTIONAL_ARG_3'][4:]

        return query.format(**kwargs).format(**kwargs)

    
    def Req_articles_cmd(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_COMMANDES.ID_COMMANDE AS ID_COMMANDE,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_COMMANDES.QTE_U AS QTE_U,	
                T_PRODUITS_COMMANDES.QTE_C AS QTE_C,	
                T_PRODUITS_COMMANDES.QTE_P AS QTE_P,	
                T_PRIX.PRIX AS PRIX_VENTE,	
                T_PRIX.CODE_AGCE AS CODE_AGCE,	
                T_ARTICLES.TYPE_CAISSE AS TYPE_CAISSE,	
                T_ARTICLES.TYPE_PALETTE AS TYPE_PALETTE
            FROM 
                T_ARTICLES,	
                T_PRIX,	
                T_PRODUITS_COMMANDES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_COMMANDES.CODE_ARTICLE
                {CODE_BLOCK_1}
        '''

        try:
            kwargs = {
                'Param_id_commande': args[0],
                'Param_code_agce': args[1],
                'param_dt': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['param_dt'] = self.validateDate(kwargs['param_dt'])

        kwargs['CODE_BLOCK_1'] = '''AND
                (
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                    {OPTIONAL_ARG_4}
                )'''
        kwargs['OPTIONAL_ARG_1'] = 'T_PRODUITS_COMMANDES.ID_COMMANDE = {Param_id_commande}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_PRIX.CODE_AGCE = {Param_code_agce}'
        kwargs['OPTIONAL_ARG_3'] = '''AND T_PRIX.Date_Debut <= '{param_dt}' '''
        kwargs['OPTIONAL_ARG_4'] = '''AND T_PRIX.Date_Fin >= '{param_dt}' '''

        if kwargs['Param_id_commande'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
            kwargs['OPTIONAL_ARG_2'] = kwargs['OPTIONAL_ARG_2'][4:]
        
        if kwargs['Param_code_agce'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_2'] = ''
            kwargs['OPTIONAL_ARG_3'] = kwargs['OPTIONAL_ARG_3'][4:]
        
        if kwargs['param_dt'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_3'] = ''
            kwargs['OPTIONAL_ARG_4'] = ''

        return query.format(**kwargs).format(**kwargs).format(**kwargs)

    
    def Req_articles_enseigne(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES_ENSEIGNE.ID_ENSEIGNE AS ID_ENSEIGNE,	
                T_ARTICLES_ENSEIGNE.CODE_ARTICLE AS CODE_ARTICLE
            FROM 
                T_ARTICLES_ENSEIGNE
            {CODE_BLOCK_1}
        '''

        try:
            kwargs = {
                'Param_ID_ENSEIGNE': args[0]
            }
        except IndexError as e:
            return e

        kwargs['CODE_BLOCK_1'] = '''WHERE 
                T_ARTICLES_ENSEIGNE.ID_ENSEIGNE = {Param_ID_ENSEIGNE}'''
        kwargs['CODE_BLOCK_1'] = '' if kwargs['Param_ID_ENSEIGNE'] in (None, 'NULL') else kwargs['CODE_BLOCK_1']

        return query.format(**kwargs).format(**kwargs)


    def Req_articles_livraison_client(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_LIVREES.QTE_COMMANDE AS QTE_COMMANDE,	
                T_PRODUITS_LIVREES.QTE_IMPORTE AS QTE_IMPORTE,	
                T_PRODUITS_LIVREES.QTE_CHARGEE AS QTE_CHARGEE,	
                T_PRODUITS_LIVREES.QTE_CAISSE AS QTE_CAISSE,	
                T_PRODUITS_LIVREES.QTE_PAL AS QTE_PAL,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_PRODUITS_LIVREES.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_LIVRAISON.STATUT AS STATUT
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                    T_LIVRAISON.STATUT <> 'A'
                )
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_type_mvt': args[1],
                'Param_code_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])
        
        kwargs['OPTIONAL_ARG_1'] = '''T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}' AND'''
        kwargs['OPTIONAL_ARG_2'] = 'T_PRODUITS_LIVREES.TYPE_MVT = {Param_type_mvt} AND'
        kwargs['OPTIONAL_ARG_3'] = 'T_LIVRAISON.CODE_CLIENT = {Param_code_client} AND'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_livraison'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_type_mvt'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_articles_livraison_secteur(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_PRODUITS_LIVREES.code_secteur AS code_secteur,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_PRODUITS_LIVREES.STATUT <> 'A'
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_livraison': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_PRODUITS_LIVREES.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_2'] = '''AND T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}' '''

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_date_livraison'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_articles_livrees(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.ORIGINE AS ORIGINE,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_THEORIQUE AS QTE_THEORIQUE,	
                T_MOUVEMENTS.QTE_REEL AS QTE_REEL,	
                T_MOUVEMENTS.QTE_ECART AS QTE_ECART,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                T_MOUVEMENTS.PRIX AS PRIX,	
                T_MOUVEMENTS.MONTANT AS MONTANT,	
                T_MOUVEMENTS.MONTANT_ECART AS MONTANT_ECART,	
                T_MOUVEMENTS.QTE_CAISSE AS QTE_CAISSE,	
                T_MOUVEMENTS.QTE_PAL AS QTE_PAL
            FROM 
                T_MOUVEMENTS
            WHERE 
                {OPTIONAL_ARG_1}
                T_MOUVEMENTS.TYPE_MOUVEMENT = 'L'
        '''

        try:
            kwargs = {
                'Param_origine': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'T_MOUVEMENTS.ORIGINE = {Param_origine} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_origine'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def req_autorisation_caisserie(self, args): #Done
        query = '''
            SELECT 
                T_AUTORISATION_SOLDE_CAISSERIE.ID_JUSTIFICATION AS ID_JUSTIFICATION,	
                T_AUTORISATION_SOLDE_CAISSERIE.DATE_HEURE AS DATE_HEURE
            FROM 
                T_AUTORISATION_SOLDE_CAISSERIE
            WHERE 
                T_AUTORISATION_SOLDE_CAISSERIE.DATE_HEURE = '{Param_dt}'
        '''
        
        try:
            kwargs = {
                'Param_dt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])

        if kwargs['Param_dt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_bl_non_envoyer(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.MOTIF_ENVOI AS MOTIF_ENVOI,	
                T_LIVRAISON.STATUT AS STATUT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_LIVRAISON.BENEFICIAIRE AS BENEFICIAIRE,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND		T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.TYPE_MVT IN ('L', 'R', 'D') 
                    AND	T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.MOTIF_ENVOI <> 1
                    AND	T_LIVRAISON.DATE_VALIDATION <> '1900-01-01 00:00:00'
                )
            GROUP BY 
                T_LIVRAISON.NUM_LIVRAISON,	
                T_LIVRAISON.DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT,	
                T_LIVRAISON.MOTIF_ENVOI,	
                T_LIVRAISON.STATUT,	
                T_CLIENTS.NOM_CLIENT,	
                T_LIVRAISON.BENEFICIAIRE
        '''
        return query

    
    def Req_bl_non_envoyer_cond(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.MOTIF_ENVOI AS MOTIF_ENVOI,	
                T_LIVRAISON.STATUT AS STATUT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_LIVRAISON.BENEFICIAIRE AS BENEFICIAIRE,	
                SUM(T_COND_LIVRAISON.QTE_CHARGEE) AS la_somme_QTE_CHARGEE
            FROM 
                T_LIVRAISON,	
                T_COND_LIVRAISON,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND		T_LIVRAISON.NUM_LIVRAISON = T_COND_LIVRAISON.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.TYPE_MVT IN ('L', 'R', 'D') 
                    AND	T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.MOTIF_ENVOI <> 1
                    AND	T_LIVRAISON.DATE_VALIDATION <> '1900-01-01 00:00:00'
                )
            GROUP BY 
                T_LIVRAISON.NUM_LIVRAISON,	
                T_LIVRAISON.DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT,	
                T_LIVRAISON.MOTIF_ENVOI,	
                T_LIVRAISON.STATUT,	
                T_CLIENTS.NOM_CLIENT,	
                T_LIVRAISON.BENEFICIAIRE
        '''
        return query

    
    def Req_bls_synch(self, args): #Done
        query = '''
            SELECT 
                T_SYNCHRO.ID_OPERATION AS ID_OPERATION
            FROM 
                T_SYNCHRO
            WHERE 
                T_SYNCHRO.OPERATION = 'BL '
        '''
        return query

    
    def Req_bordereau(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.DATE_DECOMPTE AS DATE_DECOMPTE,	
                T_DT_DECOMPTE.DATE_CHEQUE AS DATE_CHEQUE,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_DECOMPTE.REFERENCE AS REFERENCE,	
                T_DT_DECOMPTE.LE_TIRE AS LE_TIRE,	
                T_DT_DECOMPTE.DATE_ECHEANCE AS DATE_ECHEANCE,	
                T_DT_DECOMPTE.TYPE AS TYPE,	
                T_BANQUES.LIBELLE AS LIBELLE
            FROM 
                T_DECOMPTE,	
                T_DT_DECOMPTE,	
                T_BANQUES
            WHERE 
                T_DECOMPTE.NUM_DECOMPTE = T_DT_DECOMPTE.NUM_DECOMPTE
                AND		T_BANQUES.NUM_BANQUE = T_DECOMPTE.CODE_BANQUE
                AND
                (
                    T_DT_DECOMPTE.TYPE <= 2
                    AND	T_DECOMPTE.DATE_DECOMPTE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    {OPTIONAL_ARG_1}
                )
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_type': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_DECOMPTE.MODE_PAIEMENT = {Param_type}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_type'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_budget_mensuel(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                T_BUDGET_MENSUEL.DATE_BUDGET AS DATE_BUDGET,	
                T_BUDGET_MENSUEL.CODE_ARTICLE AS CODE_ARTICLE,	
                T_BUDGET_MENSUEL.QTE AS QTE
            FROM 
                T_ARTICLES,	
                T_BUDGET_MENSUEL
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_BUDGET_MENSUEL.CODE_ARTICLE
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_date_budget': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_budget'] = self.validateDate(kwargs['Param_date_budget'])

        kwargs['OPTIONAL_ARG_1'] = '''AND
                (
                    T_BUDGET_MENSUEL.DATE_BUDGET = '{Param_date_budget}'
                )'''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_budget'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ca_client_objectif(self, args): #Done
        query = '''
            SELECT 
                T_OBJECTIFS.CODE_CLIENT AS CODE_CLIENT,	
                T_OBJECTIFS.OBJECTIF AS OBJECTIF,	
                T_OBJECTIFS.REMISE_OBJECTIF AS REMISE_OBJECTIF,	
                SUM(T_FACTURE.MONTANT_FACTURE) AS la_somme_MONTANT_FACTURE,	
                T_FACTURE.DATE_HEURE AS DATE_HEURE
            FROM 
                T_FACTURE
                LEFT OUTER JOIN
                T_OBJECTIFS
                ON T_FACTURE.CODE_CLIENT = T_OBJECTIFS.CODE_CLIENT
            WHERE 
                (
                T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                {OPTIONAL_ARG_1}
            )
            GROUP BY 
                T_OBJECTIFS.CODE_CLIENT,	
                T_OBJECTIFS.OBJECTIF,	
                T_OBJECTIFS.REMISE_OBJECTIF,	
                T_FACTURE.DATE_HEURE
        '''
        
        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_code_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_DECOMPTE.MODE_PAIEMENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ca_invendu(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE * T_PRODUITS_CHARGEE.PRIX ) ) AS CA_INVENDU,
                SUM(( T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE * T_PRODUITS_CHARGEE.PRIX ) ) AS CA_ENLEV
            FROM 
                T_GAMME,	
                T_FAMILLE,	
                T_PRODUITS,	
                T_ARTICLES,	
                T_PRODUITS_CHARGEE
            WHERE 
                    T_FAMILLE.CODE_GAMME	=	T_GAMME.CODE_GAMME
                AND	T_PRODUITS.CODE_FAMILLE	=	T_FAMILLE.CODE_FAMILLE
                AND	T_ARTICLES.CODE_PRODUIT	=	T_PRODUITS.CODE_PRODUIT
                AND	T_PRODUITS_CHARGEE.CODE_ARTICLE	=	T_ARTICLES.CODE_ARTICLE
                AND
                (
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    T_GAMME.CODE_GAMME <> 1
                )
            GROUP BY 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.code_secteur
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        kwargs['OPTIONAL_ARG_1'] = '''T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}' AND'''
        kwargs['OPTIONAL_ARG_2'] = 'T_PRODUITS_CHARGEE.code_secteur = {Param_code_secteur} AND'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_chargement'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ca_invendu_periode(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE * T_PRODUITS_CHARGEE.PRIX ) ) AS CA_INVENDU,	
                SUM(( T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE * T_PRODUITS_CHARGEE.PRIX ) ) AS CA_ENLEV
            FROM 
                T_GAMME,	
                T_FAMILLE,	
                T_PRODUITS,	
                T_ARTICLES,	
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.CODE_ARTICLE = T_ARTICLES.CODE_ARTICLE
                AND		T_ARTICLES.CODE_PRODUIT = T_PRODUITS.CODE_PRODUIT
                AND		T_PRODUITS.CODE_FAMILLE = T_FAMILLE.CODE_FAMILLE
                AND		T_FAMILLE.CODE_GAMME = T_GAMME.CODE_GAMME
                AND
                (
                    T_PRODUITS_CHARGEE.DATE_CHARGEMENT BETWEEN {Param_date1} AND {Param_date2}
                    {OPTIONAL_ARG_1}
                    AND	T_GAMME.CODE_GAMME <> 1
                )
            GROUP BY 
                T_PRODUITS_CHARGEE.code_secteur
        '''
               
        try:
            kwargs = {
                'Param_date1': args[0],
                'Param_date2': args[1],
                'Param_code_secteur': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date1'] = self.validateDate(kwargs['Param_date1'], 0)
        kwargs['Param_date2'] = self.validateDate(kwargs['Param_date2'], 1)

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_PRODUITS_CHARGEE.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ca_lait_frais(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                SUM(( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX ) ) AS CA,	
                T_ARTICLES.TVA AS TVA
            FROM 
                T_DT_FACTURE,	
                T_FACTURE,	
                T_ARTICLES
            WHERE 
                T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_FACTURE.DATE_HEURE >= '{Param_dt1}'
                    AND	T_FACTURE.DATE_HEURE <= '{Param_dt2}'
                    AND	T_FACTURE.VALID = 1
                    AND	T_DT_FACTURE.CODE_ARTICLE IN (1, 2) 
                )
            GROUP BY 
                T_FACTURE.CODE_CLIENT,	
                T_ARTICLES.TVA
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        kwargs['OPTIONAL_ARG_1'] = 'T_FACTURE.CODE_CLIENT = {Param_code_client} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ca_pda(self, args): #Done
        query = '''
            SELECT 
                SUM(T_FACTURE.MONTANT_FACTURE) AS la_somme_MONTANT_FACTURE,	
                SUM(T_FACTURE.MONTANT_PERTE) AS la_somme_MONTANT_PERTE
            FROM 
                T_CLIENTS,	
                T_FACTURE,	
                T_SOUS_SECTEUR
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND
                (
                    T_FACTURE.VALID = 1
                    {OPTIONAL_ARG_1}
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ca_secteur(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                SUM(T_PRODUITS_CHARGEE.MONTANT) AS MONTANT_VENTE,	
                SUM(T_PRODUITS_CHARGEE.MONTANT_CREDIT) AS MONTANT_CREDIT
            FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                AND	T_PRODUITS_CHARGEE.code_secteur = {Param_code_secteur}
            GROUP BY 
                T_PRODUITS_CHARGEE.code_secteur
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_code_secteur': args[2]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_PRODUITS_CHARGEE.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ca_secteur_date(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                SUM(T_PRODUITS_CHARGEE.MONTANT) AS MONTANT_VENTE,	
                SUM(T_PRODUITS_CHARGEE.MONTANT_CREDIT) AS MONTANT_CREDIT,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM * T_PRODUITS_CHARGEE.PRIX ) ) AS CA_PERTE,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_REMISE * T_PRODUITS_CHARGEE.PRIX ) ) AS CA_REMISE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_CHARGEMENT.HEURE_SORTIE AS HEURE_SORTIE,	
                T_CHARGEMENT.HEURE_ENTREE AS HEURE_ENTREE,	
                T_CHARGEMENT.KM_PARCOURUS AS KM_PARCOURUS
            FROM 
                T_OPERATEUR,	
                T_CHARGEMENT,	
                T_PRODUITS_CHARGEE
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND
                (
                    T_PRODUITS_CHARGEE.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    {OPTIONAL_ARG_1}
                )
            GROUP BY 
                T_PRODUITS_CHARGEE.code_secteur,	
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_CHARGEMENT.HEURE_SORTIE,	
                T_CHARGEMENT.HEURE_ENTREE,	
                T_CHARGEMENT.KM_PARCOURUS
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_code_secteur': args[2]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_PRODUITS_CHARGEE.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ca_secteur_periode(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                SUM(T_PRODUITS_CHARGEE.MONTANT) AS la_somme_MONTANT,	
                SUM(T_PRODUITS_CHARGEE.MONTANT_CREDIT) AS la_somme_MONTANT_CREDIT,
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_POINTE * T_PRODUITS_CHARGEE.PRIX) AS CA_PERTE
            FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
            GROUP BY 
                T_PRODUITS_CHARGEE.code_secteur
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_caisses_palettes(self, args): #Done
        query = '''
            SELECT 
                T_CAISSES_PALETTES.CODE_TYPE AS CODE_TYPE,	
                T_CAISSES_PALETTES.NOM_TYPE AS NOM_TYPE,	
                T_CAISSES_PALETTES.ABREVIATION AS ABREVIATION,	
                T_CAISSES_PALETTES.CATEGORIE AS CATEGORIE,	
                T_CAISSES_PALETTES.PRIX_VENTE AS PRIX_VENTE,	
                T_CAISSES_PALETTES.PRME_RECUP AS PRME_RECUP,	
                T_CAISSES_PALETTES.RANG AS RANG
            FROM 
                T_CAISSES_PALETTES
            ORDER BY 
                RANG ASC
        '''
        return query

    
    def Req_chargement(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_CHARGEE.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.ABREVIATION AS ABREVIATION,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_ARTICLES.QTE_PALETTE AS QTE_PALETTE,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE AS QTE_CHARGEE,	
                T_PRODUITS_CHARGEE.QTE_COND AS QTE_COND,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_SUPP AS QTE_CHARGEE_SUPP,	
                T_PRODUITS_CHARGEE.QTE_ECART AS QTE_ECART,	
                T_PRODUITS_CHARGEE.TOTAL_VENDU AS TOTAL_VENDU,	
                T_PRODUITS_CHARGEE.MONTANT AS MONTANT,	
                T_PRODUITS_CHARGEE.MONTANT_CREDIT AS MONTANT_CREDIT,	
                T_PRODUITS_CHARGEE.TOTAL_GRATUIT AS TOTAL_GRATUIT,	
                T_PRODUITS_CHARGEE.TOTAL_DONS AS TOTAL_DONS,	
                T_PRODUITS_CHARGEE.TOTAL_ECHANGE AS TOTAL_ECHANGE,	
                T_PRODUITS_CHARGEE.TOTAL_REMISE AS TOTAL_REMISE,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG AS TOTAL_RENDUS_AG,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_US AS TOTAL_RENDUS_US,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM AS TOTAL_RENDUS_COM,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP AS TOTAL_RENDUS_SP,	
                T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE AS TOTAL_INVENDU_POINTE,	
                T_PRODUITS_CHARGEE.PRIX AS PRIX,	
                T_PRODUITS_CHARGEE.PRIX_VNT AS PRIX_VNT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE AS CODE_TOURNEE,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_CHARGEMENT.VALID AS VALID,	
                T_CHARGEMENT.MONTANT_A_VERSER AS MONTANT_A_VERSER,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE AS QTE_CHARGEE_POINTE,	
                T_PRODUITS_CHARGEE.CREDIT AS CREDIT
            FROM 
                T_CHARGEMENT,	
                T_PRODUITS_CHARGEE,	
                T_ARTICLES
            WHERE 
                T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_CHARGEE.CODE_ARTICLE
                AND
                (
                    T_ARTICLES.ACTIF = 1
                    AND	T_PRODUITS_CHARGEE.CODE_CHARGEMENT = {Param_code_chargement}
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_code_chargement': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_code_chargement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_chargement_article(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_CHARGEE) AS la_somme_TOTAL_CHARGEE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_VENDU) AS la_somme_TOTAL_VENDU,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_GRATUIT) AS la_somme_TOTAL_GRATUIT,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_DONS) AS la_somme_TOTAL_DONS,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_REMISE) AS la_somme_TOTAL_REMISE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_POINTE) AS la_somme_TOTAL_RENDUS_POINTE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE) AS la_somme_TOTAL_INVENDU_POINTE,	
                SUM(T_PRODUITS_CHARGEE.CMD_U) AS la_somme_CMD_U,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM) AS la_somme_TOTAL_RENDUS_COM,	
                T_CHARGEMENT.CHARGEMENT_CAC AS CHARGEMENT_CAC,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_PRODUITS_CHARGEE.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_CHARGEE.CREDIT) AS la_somme_CREDIT,	
                T_CHARGEMENT.code_secteur AS CODE_SECTEUR_T_,	
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.AIDE_VENDEUR1 AS AIDE_VENDEUR1,	
                T_CHARGEMENT.AIDE_VENDEUR2 AS AIDE_VENDEUR2,	
                T_CHARGEMENT.code_chauffeur AS code_chauffeur,	
                T_CHARGEMENT.vehicule AS vehicule,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_US) AS la_somme_TOTAL_RENDUS_US,	
                T_CHARGEMENT.CODE_CHARGEMENT AS CODE_CHARGEMENT
            FROM 
                T_CHARGEMENT,	
                T_PRODUITS_CHARGEE,	
                T_OPERATEUR
            WHERE 
                T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND
                (
                    T_CHARGEMENT.code_secteur = {Param_code_secteur}
                    AND	T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                )
            GROUP BY 
                T_CHARGEMENT.code_vendeur,	
                T_CHARGEMENT.CHARGEMENT_CAC,	
                T_OPERATEUR.FONCTION,	
                T_PRODUITS_CHARGEE.CODE_ARTICLE,	
                T_CHARGEMENT.code_secteur,	
                T_CHARGEMENT.DATE_CHARGEMENT,	
                T_CHARGEMENT.AIDE_VENDEUR1,	
                T_CHARGEMENT.AIDE_VENDEUR2,	
                T_CHARGEMENT.code_chauffeur,	
                T_CHARGEMENT.vehicule,	
                T_CHARGEMENT.CODE_CHARGEMENT
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_chargement': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_chargement_cond(self, args): #Done
        query = '''
            SELECT 
                T_COND_CHARGEE.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                T_COND_CHARGEE.QTE_CHARGEE AS QTE_CHARGEE,	
                T_COND_CHARGEE.QTE_CHARGEE_VAL AS QTE_CHARGEE_VAL,	
                T_COND_CHARGEE.QTE_RETOUR AS QTE_RETOUR,	
                T_COND_CHARGEE.CREDIT AS CREDIT,	
                T_CAISSES_PALETTES.NOM_TYPE AS NOM_TYPE,	
                T_CAISSES_PALETTES.ABREVIATION AS ABREVIATION,	
                T_COND_CHARGEE.QTE_CHAR_SUPP AS QTE_CHAR_SUPP,	
                T_COND_CHARGEE.QTE_POINTE AS QTE_POINTE,	
                T_COND_CHARGEE.ECART AS ECART
            FROM 
                T_CAISSES_PALETTES,	
                T_COND_CHARGEE
            WHERE 
                T_CAISSES_PALETTES.CODE_TYPE = T_COND_CHARGEE.CODE_COND
                AND
                (
                    T_COND_CHARGEE.CODE_CHARGEMENT = {Param_code_chargement}
                )
        '''

        try:
            kwargs = {
                'Param_code_chargement': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_chargement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_chargement_non_valide(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                COUNT(T_CHARGEMENT.CODE_CHARGEMENT) AS Comptage_1,	
                T_CHARGEMENT.VALID AS VALID,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR
            FROM 
                T_SECTEUR,	
                T_CHARGEMENT
            WHERE 
                T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_CHARGEMENT.VALID = 0
                )
            GROUP BY 
                T_CHARGEMENT.DATE_CHARGEMENT,	
                T_CHARGEMENT.VALID,	
                T_SECTEUR.NOM_SECTEUR
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])
        kwargs['OPTIONAL_ARG_1'] = '''T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}' AND'''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_chargement'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_chargement_par_article(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_CHARGEE.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.ABREVIATION AS ABREVIATION,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_ARTICLES.QTE_PALETTE AS QTE_PALETTE,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE AS QTE_CHARGEE,	
                T_PRODUITS_CHARGEE.QTE_COND AS QTE_COND,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_SUPP AS QTE_CHARGEE_SUPP,	
                T_PRODUITS_CHARGEE.QTE_ECART AS QTE_ECART,	
                T_PRODUITS_CHARGEE.TOTAL_VENDU AS TOTAL_VENDU,	
                T_PRODUITS_CHARGEE.MONTANT AS MONTANT,	
                T_PRODUITS_CHARGEE.MONTANT_CREDIT AS MONTANT_CREDIT,	
                T_PRODUITS_CHARGEE.TOTAL_GRATUIT AS TOTAL_GRATUIT,	
                T_PRODUITS_CHARGEE.TOTAL_DONS AS TOTAL_DONS,	
                T_PRODUITS_CHARGEE.TOTAL_ECHANGE AS TOTAL_ECHANGE,	
                T_PRODUITS_CHARGEE.TOTAL_REMISE AS TOTAL_REMISE,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG AS TOTAL_RENDUS_AG,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_US AS TOTAL_RENDUS_US,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM AS TOTAL_RENDUS_COM,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP AS TOTAL_RENDUS_SP,	
                T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE AS TOTAL_INVENDU_POINTE,	
                T_PRODUITS_CHARGEE.PRIX AS PRIX,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE AS CODE_TOURNEE,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_CHARGEMENT.VALID AS VALID,	
                T_CHARGEMENT.MONTANT_A_VERSER AS MONTANT_A_VERSER,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE AS QTE_CHARGEE_POINTE,	
                T_PRODUITS_CHARGEE.CREDIT AS CREDIT,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR
            FROM 
                T_ARTICLES,	
                T_PRODUITS_CHARGEE,	
                T_CHARGEMENT,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_CHARGEE.CODE_ARTICLE
                AND
                (
                    T_ARTICLES.ACTIF = 1
                    AND	T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date_chragement}'
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_date_chragement': args[0],
                'Param_code_secteur': args[1],
                'Param_code_article': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chragement'] = self.validateDate(kwargs['Param_date_chragement'])
        
        if kwargs['Param_date_chragement'] in (None, 'NULL'):
            return ValueError

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_ARTICLES.CODE_ARTICLE = {Param_code_article}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_article'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_chargement_par_produit(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                MAX(T_ARTICLES.RANG) AS le_maximum_RANG,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE AS CODE_TOURNEE,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_CHARGEMENT.VALID AS VALID,	
                T_CHARGEMENT.MONTANT_A_VERSER AS MONTANT_A_VERSER,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                SUM(T_PRODUITS_CHARGEE.QTE_COND) AS la_somme_QTE_COND,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE_SUPP) AS la_somme_QTE_CHARGEE_SUPP,	
                SUM(T_PRODUITS_CHARGEE.QTE_ECART) AS la_somme_QTE_ECART,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_VENDU) AS la_somme_TOTAL_VENDU,	
                SUM(T_PRODUITS_CHARGEE.MONTANT) AS la_somme_MONTANT,	
                SUM(T_PRODUITS_CHARGEE.MONTANT_CREDIT) AS la_somme_MONTANT_CREDIT,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_GRATUIT) AS la_somme_TOTAL_GRATUIT,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_DONS) AS la_somme_TOTAL_DONS,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_ECHANGE) AS la_somme_TOTAL_ECHANGE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_REMISE) AS la_somme_TOTAL_REMISE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG) AS la_somme_TOTAL_RENDUS_AG,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_US) AS la_somme_TOTAL_RENDUS_US,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM) AS la_somme_TOTAL_RENDUS_COM,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP) AS la_somme_TOTAL_RENDUS_SP,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE) AS la_somme_TOTAL_INVENDU_POINTE,	
                MAX(T_PRODUITS_CHARGEE.PRIX) AS le_maximum_PRIX,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE) AS la_somme_QTE_CHARGEE_POINTE,	
                SUM(T_PRODUITS_CHARGEE.CREDIT) AS la_somme_CREDIT
            FROM 
                T_CHARGEMENT,	
                T_PRODUITS_CHARGEE,	
                T_SECTEUR,	
                T_ARTICLES,	
                T_PRODUITS
            WHERE 
                T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_CHARGEE.CODE_ARTICLE
                AND		T_SECTEUR.code_secteur = T_PRODUITS_CHARGEE.code_secteur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND
                (
                    T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date_chragement}'
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
            GROUP BY 
                T_PRODUITS_CHARGEE.CODE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE,	
                T_CHARGEMENT.code_vendeur,	
                T_CHARGEMENT.VALID,	
                T_CHARGEMENT.MONTANT_A_VERSER,	
                T_SECTEUR.NOM_SECTEUR,	
                T_PRODUITS.NOM_PRODUIT,	
                T_PRODUITS.CODE_PRODUIT
            ORDER BY 
                le_maximum_RANG ASC
        '''

        try:
            kwargs = {
                'Param_date_chragement': args[0],
                'Param_code_secteur': args[1],
                'Param_code_produit': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chragement'] = self.validateDate(kwargs['Param_date_chragement'])
        
        if kwargs['Param_date_chragement'] in (None, 'NULL'):
            return ValueError

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_PRODUITS.CODE_PRODUIT = {Param_code_produit}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_produit'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_chargement_periode(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR
            FROM 
                T_OPERATEUR,	
                T_CHARGEMENT,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT BETWEEN '{Param_date1}' AND '{Param_date2}'
                    AND	T_CHARGEMENT.code_vendeur = {Param_code_vendeur}
                )
            ORDER BY 
                DATE_CHARGEMENT ASC
        '''

        try:
            kwargs = {
                'Param_date1': args[0],
                'Param_date2': args[1],
                'Param_code_vendeur': args[2]
            }
        except IndexError as e:
            return e

        kwargs['Param_date1'] = self.validateDate(kwargs['Param_date1'])
        kwargs['Param_date2'] = self.validateDate(kwargs['Param_date2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_chargement_secteur(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                T_PRODUITS_CHARGEE.TOTAL_CHARGEE AS TOTAL_CHARGEE,	
                T_PRODUITS_CHARGEE.TOTAL_GRATUIT AS TOTAL_GRATUIT,	
                T_PRODUITS_CHARGEE.TOTAL_DONS AS TOTAL_DONS,	
                T_PRODUITS_CHARGEE.TOTAL_REMISE AS TOTAL_REMISE,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG AS TOTAL_RENDUS_AG,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_US AS TOTAL_RENDUS_US,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM AS TOTAL_RENDUS_COM,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP AS TOTAL_RENDUS_SP,	
                T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE AS TOTAL_INVENDU_POINTE,	
                T_PRODUITS_CHARGEE.PRIX AS PRIX,	
                T_PRODUITS_CHARGEE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_PRODUITS_CHARGEE.CREDIT AS CREDIT,	
                T_PRODUITS_CHARGEE.QTE_ECART AS QTE_ECART,	
                T_CHARGEMENT.code_vendeur AS code_vendeur
            FROM 
                T_CHARGEMENT,	
                T_PRODUITS_CHARGEE,	
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                {CODE_BLOCK_1}
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        kwargs['CODE_BLOCK_1'] = '''AND
                (
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )'''
        kwargs['OPTIONAL_ARG_1'] = '''T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}' '''
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_PRODUITS_CHARGEE.code_secteur = {Param_code_secteur}'

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
            kwargs['OPTIONAL_ARG_2'] = kwargs['OPTIONAL_ARG_2'][4:]
        
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['CODE_BLOCK_1'] = '' if kwargs['OPTIONAL_ARG_1'] == '' and kwargs['OPTIONAL_ARG_2'] == '' else kwargs['CODE_BLOCK_1']

        return query.format(**kwargs).format(**kwargs).format(**kwargs)

    
    def Req_cheque_non_envoyer(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.NUM_DECOMPTE AS NUM_DECOMPTE,	
                T_DECOMPTE.DATE_DECOMPTE AS DATE_DECOMPTE,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_DECOMPTE.REFERENCE AS REFERENCE,	
                T_DECOMPTE.CODE_CLIENT AS CODE_CLIENT,	
                T_DT_DECOMPTE.GP_CLIENT AS GP_CLIENT,	
                T_DT_DECOMPTE.LE_TIRE AS LE_TIRE,	
                T_DT_DECOMPTE.TYPE AS TYPE,	
                T_DT_DECOMPTE.MOTIF_ENVOI AS MOTIF_ENVOI,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT
            FROM 
                T_DT_DECOMPTE,	
                T_DECOMPTE,	
                T_CLIENTS
            WHERE 
                T_DECOMPTE.NUM_DECOMPTE = T_DT_DECOMPTE.NUM_DECOMPTE
                AND		T_CLIENTS.CODE_CLIENT = T_DECOMPTE.CODE_CLIENT
                AND
                (
                    T_DT_DECOMPTE.MOTIF_ENVOI <> 1
                )
        '''
        return query

    
    def Req_client_cac_journee(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur
            FROM 
                T_LIVRAISON,	
                T_SOUS_SECTEUR,	
                T_CLIENTS
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_LIVRAISON.TYPE_MVT IN ('L', 'R') 
                    AND	T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                )
        '''

        try:
            kwargs = {
                'Param_dt': args[0],
                'Param_date_livraison': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        kwargs['OPTIONAL_ARG_1'] = '''T_LIVRAISON.DATE_VALIDATION = '{Param_dt}' AND'''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_dt'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_client_not_int(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.ADRESSE AS ADRESSE,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur
            FROM 
                T_SOUS_SECTEUR,	
                T_CLIENTS
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_CLIENTS.ACTIF = 1
                    AND	T_CLIENTS.CODE_CLIENT <> 0
                    AND	T_CLIENTS.CODE_CLIENT NOT IN ({Param_code_clt}) 
                    AND	T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
                )
        '''

        try:
            kwargs = {
                'Param_code_clt': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_clients_n1(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_MOY_VENTE_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_MOY_VENTE_CLIENTS.DATE_VENTE AS DATE_VENTE
            FROM 
                T_MOY_VENTE_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_MOY_VENTE_CLIENTS.CODE_CLIENT
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_MOY_VENTE_CLIENTS.DATE_VENTE = '{Param_date}'
                )
        '''

        try:
            kwargs = {
                'Param_date': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])

        if kwargs['Param_date'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_code_chargement(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur
            FROM 
                T_CHARGEMENT
            WHERE 
                T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_codification_operation(self, args): #Done
        query = '''
            SELECT 
                MAX(T_OPERATIONS.CODE_OPERATION) AS le_maximum_CODE_OPERATION
            FROM 
                T_OPERATIONS
        '''
        return query

    
    def Req_commande_gms_date(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.CODE_CLIENT AS CODE_CLIENT,	
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COMMANDES.code_secteur AS code_secteur,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                SUM(T_PRODUITS_COMMANDES.QTE_U) AS la_somme_QTE_U,	
                SUM(T_PRODUITS_COMMANDES.QTE_C) AS la_somme_QTE_C,	
                T_COMMANDES.TYPE_COMMANDE AS TYPE_COMMANDE
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES,	
                T_ARTICLES
            WHERE 
                T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_COMMANDES.CODE_ARTICLE
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}'
                    {OPTIONAL_ARG_2}
                    AND	T_COMMANDES.TYPE_COMMANDE = 'C'
                )
            GROUP BY 
                T_COMMANDES.CODE_CLIENT,	
                T_COMMANDES.DATE_LIVRAISON,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE,	
                T_ARTICLES.LIBELLE_COURT,	
                T_COMMANDES.code_secteur,	
                T_COMMANDES.TYPE_COMMANDE
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_date_livraison': args[1],
                'Param_code_secteur': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])
        
        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        kwargs['OPTIONAL_ARG_1'] = 'T_COMMANDES.CODE_CLIENT = {Param_code_client} AND'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_COMMANDES.code_secteur = {Param_code_secteur}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_commande_secteur(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COMMANDES.code_secteur AS code_secteur,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_COMMANDES.QTE_U) AS la_somme_QTE_U,	
                SUM(T_PRODUITS_COMMANDES.QTE_C) AS la_somme_QTE_C,	
                T_COMMANDES.TYPE_COMMANDE AS TYPE_COMMANDE
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES
            WHERE 
                T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND
                (
                    T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_COMMANDES.code_secteur = {Param_code_secteur}
                    AND	T_COMMANDES.TYPE_COMMANDE = 'S'
                )
            GROUP BY 
                T_COMMANDES.DATE_LIVRAISON,	
                T_COMMANDES.code_secteur,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE,	
                T_COMMANDES.TYPE_COMMANDE
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_commande_secteur_article(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COMMANDES.code_secteur AS code_secteur,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_COMMANDES.QTE_U) AS la_somme_QTE_U,	
                SUM(T_PRODUITS_COMMANDES.QTE_C) AS la_somme_QTE_C
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES
            WHERE 
                T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND
                (
                    T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_COMMANDES.code_secteur = {Param_code_secteur}
                    AND	T_PRODUITS_COMMANDES.CODE_ARTICLE = {Param_code_article}
                )
            GROUP BY 
                T_COMMANDES.DATE_LIVRAISON,	
                T_COMMANDES.code_secteur,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_secteur': args[1],
                'Param_code_article': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_commande_secteur_produit(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COMMANDES.code_secteur AS code_secteur,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                SUM(T_PRODUITS_COMMANDES.QTE_U) AS la_somme_QTE_U,	
                SUM(T_PRODUITS_COMMANDES.QTE_C) AS la_somme_QTE_C
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES,	
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRODUITS_COMMANDES.CODE_ARTICLE
                AND		T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND
                (
                    T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_COMMANDES.code_secteur = {Param_code_secteur}
                    AND	T_ARTICLES.CODE_PRODUIT = {Param_code_produit}
                )
            GROUP BY 
                T_COMMANDES.DATE_LIVRAISON,	
                T_COMMANDES.code_secteur,	
                T_ARTICLES.CODE_PRODUIT
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_secteur': args[1],
                'Param_code_produit': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_commande_usine_date(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_COMMANDES.TYPE_COMMANDE AS TYPE_COMMANDE,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                SUM(T_PRODUITS_COMMANDES.QTE_U) AS la_somme_QTE_U,	
                SUM(T_PRODUITS_COMMANDES.QTE_C) AS la_somme_QTE_C,	
                T_ARTICLES.RANG AS RANG
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES,	
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRODUITS_COMMANDES.CODE_ARTICLE
                AND		T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND
                (
                    T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_COMMANDES.TYPE_COMMANDE = 'U'
                )
            GROUP BY 
                T_COMMANDES.DATE_LIVRAISON,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE,	
                T_ARTICLES.LIBELLE_COURT,	
                T_COMMANDES.TYPE_COMMANDE,	
                T_ARTICLES.RANG
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_cond_charge_operateur(self, args): #Done
        query = '''
            SELECT 
                T_COND_CHARGEE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_COND_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                SUM(T_COND_CHARGEE.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                SUM(T_COND_CHARGEE.QTE_RETOUR) AS la_somme_QTE_RETOUR,	
                SUM(T_COND_CHARGEE.QTE_CHAR_SUPP) AS la_somme_QTE_CHAR_SUPP,	
                SUM(T_COND_CHARGEE.QTE_POINTE) AS la_somme_QTE_POINTE,	
                SUM(T_COND_CHARGEE.CREDIT) AS la_somme_CREDIT
            FROM 
                T_COND_CHARGEE
            WHERE 
                {OPTIONAL_ARG_1}
                T_COND_CHARGEE.DATE_CHARGEMENT = '{Param_date_mvt}'
            GROUP BY 
                T_COND_CHARGEE.CODE_OPERATEUR,	
                T_COND_CHARGEE.CODE_COND,	
                T_COND_CHARGEE.DATE_CHARGEMENT
        '''

        try:
            kwargs = {
                'Param_code_cond': args[0],
                'Param_date_mvt': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        kwargs['OPTIONAL_ARG_1'] = 'T_COND_CHARGEE.CODE_COND = {Param_code_cond} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_cond'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_cond_chargee(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_COND_CHARGEE.code_secteur AS code_secteur,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                T_COND_CHARGEE.QTE_CHARGEE AS QTE_CHARGEE,	
                T_COND_CHARGEE.QTE_CHARGEE_VAL AS QTE_CHARGEE_VAL,	
                T_COND_CHARGEE.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_COND_CHARGEE.ECART AS ECART,	
                T_COND_CHARGEE.QTE_CHAR_SUPP AS QTE_CHAR_SUPP,	
                T_COND_CHARGEE.CREDIT AS CREDIT,	
                T_COND_CHARGEE.QTE_POINTE AS QTE_POINTE
            FROM 
                T_CHARGEMENT,	
                T_COND_CHARGEE
            WHERE 
                T_CHARGEMENT.CODE_CHARGEMENT = T_COND_CHARGEE.CODE_CHARGEMENT
                AND
                (
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                )
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Paramcode_cp': args[1],
                'Param_date_chargement': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_COND_CHARGEE.code_secteur = {Param_code_secteur} AND'
        kwargs['OPTIONAL_ARG_2'] = 'T_COND_CHARGEE.CODE_COND = {Paramcode_cp} AND'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Paramcode_cp'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_cond_livraison(self, args): #Done
        query = '''
            UPDATE 
                T_COND_LIVRAISON
            SET
                QTE_IMPORTE = {Param_qte_chargee},	
                QTE_CHARGEE = {Param_qte_chargee}
            WHERE 
                T_COND_LIVRAISON.NUM_LIVRAISON = {Param_num_livraison}
                AND	T_COND_LIVRAISON.CODE_CP = {Param_code_cp}
        '''

        try:
            kwargs = {
                'Param_qte_chargee': args[0],
                'Param_num_livraison': args[1],
                'Param_code_cp': args[2]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_cond_livraison_client(self, args): #Done
        query = '''
            SELECT 
                T_COND_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                SUM(T_COND_LIVRAISON.QTE_IMPORTE) AS la_somme_QTE_IMPORTE
            FROM 
                T_LIVRAISON,	
                T_COND_LIVRAISON
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_COND_LIVRAISON.NUM_LIVRAISON
                AND
                (
                    T_COND_LIVRAISON.CODE_CLIENT = {Param_code_client}
                    AND	T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
            GROUP BY 
                T_COND_LIVRAISON.CODE_CLIENT,	
                T_LIVRAISON.DATE_LIVRAISON,	
                T_COND_LIVRAISON.CODE_CP
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_date_livraison': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def req_conseigne_deconseige(self, args): #Done
        query = '''
            SELECT 
                T_REGELEMENT_COND.DATE_VALIDATION AS DATE_VALIDATION,	
                T_REGELEMENT_COND.CODE_OPERTAEUR AS CODE_OPERTAEUR,	
                SUM(T_REGELEMENT_COND.REGLER_C_STD) AS la_somme_REGLER_C_STD,	
                SUM(T_REGELEMENT_COND.MONTANT_REGLER) AS la_somme_MONTANT_REGLER,	
                SUM(T_REGELEMENT_COND.REGLER_P_AG) AS la_somme_REGLER_P_AG,	
                SUM(T_REGELEMENT_COND.REGLER_P_UHT) AS la_somme_REGLER_P_UHT,	
                SUM(T_REGELEMENT_COND.REGLER_C_AG) AS la_somme_REGLER_C_AG,	
                SUM(T_REGELEMENT_COND.REGLER_C_PR) AS la_somme_REGLER_C_PR,	
                SUM(T_REGELEMENT_COND.REGLER_C_BLC) AS la_somme_REGLER_C_BLC,	
                SUM(T_REGELEMENT_COND.REGLER_P_EURO) AS la_somme_REGLER_P_EURO
            FROM 
                T_REGELEMENT_COND
            WHERE 
                T_REGELEMENT_COND.CODE_OPERTAEUR = {Param_code_operateur}
                AND	T_REGELEMENT_COND.DATE_VALIDATION BETWEEN '{Param_dt1}' AND '{Param_dt2}'
            GROUP BY 
                T_REGELEMENT_COND.CODE_OPERTAEUR,	
                T_REGELEMENT_COND.DATE_VALIDATION
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_contrubition_canal(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                T_STATISTIQUES.CATEGORIE AS CATEGORIE,	
                SUM(( T_STATISTIQUES.VENTE + T_STATISTIQUES.VENTE_CAC ) ) AS la_somme_VENTE
            FROM 
                T_ARTICLES,	
                T_STATISTIQUES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_STATISTIQUES.CODE_ARTICLE
                AND
                (
                    T_STATISTIQUES.DATE_JOURNEE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_STATISTIQUES.CATEGORIE,	
                T_ARTICLES.CODE_PRODUIT
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_date_dispo_statistiques(self, args): #Done
        query = '''
            SELECT 
                MAX(T_STATISTIQUES.DATE_JOURNEE) AS le_maximum_DATE_JOURNEE
            FROM 
                T_STATISTIQUES
            WHERE 
                T_STATISTIQUES.DATE_JOURNEE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_date_distribution_remise(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                MAX(T_FACTURE.DATE_HEURE) AS le_maximum_DATE_HEURE,	
                SUM(( T_DT_FACTURE.QTE_REMISE * T_DT_FACTURE.PRIX ) ) AS MT_REPARTI
            FROM 
                T_FACTURE,	
                T_DT_FACTURE,	
                T_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_SECTEUR,	
                T_BLOC,	
                T_ZONE
            WHERE 
                T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_DT_FACTURE.QTE_REMISE > 0
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                )
            GROUP BY 
                T_FACTURE.CODE_CLIENT
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_code_secteur': args[2],
                'Param_code_superviseur': args[3],
                'Param_resp_vente': args[4]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in ('Param_dt1', 'Param_dt2'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SECTEUR.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}'
        kwargs['OPTIONAL_ARG_3'] = 'AND	T_ZONE.RESP_VENTE = {Param_resp_vente}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_superviseur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['Param_resp_vente'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_decompte_operateur_journee(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_DECOMPTE.DATE_DECOMPTE AS DATE_DECOMPTE,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT,	
                T_DECOMPTE.MONTANT AS MONTANT
            FROM 
                T_DECOMPTE
            WHERE 
                T_DECOMPTE.CODE_OPERATEUR = {Param_code_operateur}
                AND	T_DECOMPTE.DATE_DECOMPTE = '{Param_date_decompte}'
                AND	T_DECOMPTE.MODE_PAIEMENT = 'E'
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0],
                'Param_date_decompte': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_decompte'] = self.validateDate(kwargs['Param_date_decompte'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_dernier_chargement(self, args): #Done
        query = '''
            SELECT TOP 5 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE AS CODE_TOURNEE
            FROM 
                T_CHARGEMENT
            WHERE 
                T_CHARGEMENT.code_secteur = {Param_code_secteur}
            ORDER BY 
                DATE_CHARGEMENT DESC
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_dernier_maj_stock(self, args): #Done
        query = '''
            SELECT 
                T_SECTEUR.code_secteur AS code_secteur,	
                T_SECTEUR.DERNIER_MAJ AS DERNIER_MAJ
            FROM 
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = {Param_cde_secteur}
        '''

        try:
            kwargs = {
                'Param_cde_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_cde_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_dernier_rib(self, args): #Done
        query = '''
            SELECT DISTINCT TOP 5 
                T_DECOMPTE.CODE_CLIENT AS CODE_CLIENT,	
                T_DT_DECOMPTE.GP_CLIENT AS GP_CLIENT,	
                T_DT_DECOMPTE.RIB AS RIB
            FROM 
                T_DECOMPTE,	
                T_DT_DECOMPTE
            WHERE 
                T_DECOMPTE.NUM_DECOMPTE = T_DT_DECOMPTE.NUM_DECOMPTE
                AND
                (
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_gp_client': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'T_DECOMPTE.CODE_CLIENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_DT_DECOMPTE.GP_CLIENT = {Param_gp_client}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_gp_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs)

    
    def Req_det_borderau_valeurs(self, args): #Done
        query = '''
            SELECT 
                T_DT_BORDEREAU.ID_BORDEREAU AS ID_BORDEREAU,	
                T_DT_BORDEREAU.NUM_DECOMPTE AS NUM_DECOMPTE,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_DECOMPTE.REFERENCE AS REFERENCE,	
                T_DT_DECOMPTE.LE_TIRE AS LE_TIRE
            FROM 
                T_DECOMPTE,	
                T_DT_DECOMPTE,	
                T_DT_BORDEREAU
            WHERE 
                T_DECOMPTE.NUM_DECOMPTE = T_DT_BORDEREAU.NUM_DECOMPTE
                AND		T_DECOMPTE.NUM_DECOMPTE = T_DT_DECOMPTE.NUM_DECOMPTE
                AND
                (
                    T_DT_BORDEREAU.ID_BORDEREAU = {Param_id_bordereau}
                )
        '''

        try:
            kwargs = {
                'Param_id_bordereau': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_bordereau'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_dt_articles(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.CODE_BARRE AS CODE_BARRE,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.ABREVIATION AS ABREVIATION,	
                T_ARTICLES.GP_ARTICLE AS GP_ARTICLE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                T_ARTICLES.CODE_AROME AS CODE_AROME,	
                T_ARTICLES.TYPE_CAISSE AS TYPE_CAISSE,	
                T_ARTICLES.TYPE_PALETTE AS TYPE_PALETTE,	
                T_ARTICLES.TVA AS TVA,	
                T_ARTICLES.AFF_REPARTITION AS AFF_REPARTITION,	
                T_ARTICLES.AFF_COMMANDE AS AFF_COMMANDE,	
                T_ARTICLES.QTE_PACK AS QTE_PACK,	
                T_ARTICLES.QTE_PALETTE AS QTE_PALETTE,	
                T_ARTICLES.POIDS AS POIDS,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_PRIX.PRIX AS PRIX_VENTE,	
                T_PRIX.CODE_AGCE AS CODE_AGCE,	
                T_FAMILLE.CODE_FAMILLE AS CODE_FAMILLE,	
                T_FAMILLE.CODE_GAMME AS CODE_GAMME,	
                T_ARTICLES.ACTIF_GLOBALE AS ACTIF_GLOBALE,	
                T_ARTICLES.CONDITIONNEMENT2 AS CONDITIONNEMENT2,	
                T_ARTICLES.QTE_PALETTE2 AS QTE_PALETTE2,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                T_FAMILLE.NOM_FAMILLE AS NOM_FAMILLE,	
                T_GAMME.NOM_GAMME AS NOM_GAMME
            FROM 
                T_GAMME,	
                T_FAMILLE,	
                T_PRODUITS,	
                T_ARTICLES,	
                T_PRIX
            WHERE 
                T_GAMME.CODE_GAMME = T_FAMILLE.CODE_GAMME
                AND		T_FAMILLE.CODE_FAMILLE = T_PRODUITS.CODE_FAMILLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND
                (
                    T_PRIX.CODE_AGCE = {Param_code_agence}
                    AND	T_ARTICLES.ACTIF_GLOBALE = 1
                    AND	T_PRIX.Date_Debut <= '{param_dt}'
                    AND	T_PRIX.Date_Fin >= '{param_dt}'
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_code_agence': args[0],
                'param_dt': args[1]
            }
        except IndexError as e:
            return e

        kwargs['param_dt'] = self.validateDate(kwargs['param_dt'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_dt_caisserie_secteur(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                T_COND_CHARGEE.QTE_CHARGEE_VAL AS QTE_CHARGEE_VAL,	
                T_COND_CHARGEE.QTE_CHAR_SUPP AS QTE_CHAR_SUPP,	
                T_COND_CHARGEE.QTE_RETOUR AS QTE_RETOUR
            FROM 
                T_CHARGEMENT,	
                T_COND_CHARGEE,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_COND_CHARGEE.CODE_CHARGEMENT
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_COND_CHARGEE.CODE_OPERATEUR = {Param_code_operateur}
                )
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_code_operateur': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_dt_courrier(self, args): #Done
        query = '''
            SELECT 
                T_DT_COURRIER_AGENCE.ID_ENVOI AS ID_ENVOI,	
                T_DT_COURRIER_AGENCE.TYPE_OP AS TYPE_OP,	
                T_DT_COURRIER_AGENCE.SOUS_TYPE AS SOUS_TYPE,	
                T_DT_COURRIER_AGENCE.ID_OPERATION AS ID_OPERATION,	
                T_DT_COURRIER_AGENCE.ID_AFFICHER AS ID_AFFICHER
            FROM 
                T_DT_COURRIER_AGENCE
            WHERE 
                T_DT_COURRIER_AGENCE.ID_ENVOI = {Param_id_envoi}
        '''

        try:
            kwargs = {
                'Param_id_envoi': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_envoi'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_dt_decompte(self, args): #Done
        query = '''
            UPDATE 
                T_DT_DECOMPTE
            SET
                MOTIF_ENVOI = {Param_motif}
            WHERE 
                T_DT_DECOMPTE.NUM_DECOMPTE = {Param_num_decompte}
        '''

        try:
            kwargs = {
                'Param_motif': args[0],
                'Param_num_decompte': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_dt_facture_client(self, args): #Done
        query = '''
            SELECT 
                T_DT_FACTURE.NUM_FACTURE AS NUM_FACTURE,	
                T_DT_FACTURE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_DT_FACTURE.PRIX AS PRIX,	
                T_DT_FACTURE.QTE_VENTE AS QTE_VENTE,	
                T_DT_FACTURE.QTE_PERTE AS QTE_PERTE,	
                T_DT_FACTURE.QTE_PROMO AS QTE_PROMO,	
                T_DT_FACTURE.QTE_REMISE AS QTE_REMISE,	
                T_DT_FACTURE.TX_GRATUIT AS TX_GRATUIT,	
                T_ARTICLES.TVA AS TVA
            FROM 
                T_ARTICLES,	
                T_DT_FACTURE
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND
                (
                    T_DT_FACTURE.NUM_FACTURE = {Param_num_facture}
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_num_facture': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_num_facture'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_dt_mouvement_cond(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS_CAISSERIE.ID_MOUVEMENT AS ID_MOUVEMENT,	
                T_MOUVEMENTS_CAISSERIE.CODE_MAGASIN AS CODE_MAGASIN,	
                T_MOUVEMENTS_CAISSERIE.CODE_CP AS CODE_CP,	
                T_MOUVEMENTS_CAISSERIE.ORIGINE AS ORIGINE,	
                T_MOUVEMENTS_CAISSERIE.QTE_THEORIQUE AS QTE_THEORIQUE,	
                T_MOUVEMENTS_CAISSERIE.QTE_REEL AS QTE_REEL,	
                T_MOUVEMENTS_CAISSERIE.QTE_ECART AS QTE_ECART,	
                T_CAISSES_PALETTES.NOM_TYPE AS NOM_TYPE,	
                T_MOUVEMENTS_CAISSERIE.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                T_MOUVEMENTS_CAISSERIE.PRIX AS PRIX,	
                T_MOUVEMENTS_CAISSERIE.MONTANT_ECART AS MONTANT_ECART,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_MOUVEMENTS_CAISSERIE.COMPTE_ECART AS COMPTE_ECART
            FROM 
                T_OPERATEUR,	
                T_MOUVEMENTS_CAISSERIE,	
                T_CAISSES_PALETTES
            WHERE 
                T_CAISSES_PALETTES.CODE_TYPE = T_MOUVEMENTS_CAISSERIE.CODE_CP
                AND		T_OPERATEUR.CODE_OPERATEUR = T_MOUVEMENTS_CAISSERIE.COMPTE_ECART
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_origine': args[0]
            }
        except IndexError as e:
            return e

        kwargs['OPTIONAL_ARG_1'] = '''AND
                (
                    T_MOUVEMENTS_CAISSERIE.ORIGINE = {Param_origine}
                )'''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_origine'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_dt_operation(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.ID_MOUVEMENT AS ID_MOUVEMENT,	
                T_MOUVEMENTS.ORIGINE AS ORIGINE,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_THEORIQUE AS QTE_THEORIQUE,	
                T_MOUVEMENTS.QTE_REEL AS QTE_REEL,	
                T_MOUVEMENTS.QTE_ECART AS QTE_ECART,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_MOUVEMENTS.QTE_CAISSE AS QTE_CAISSE,	
                T_MOUVEMENTS.QTE_PAL AS QTE_PAL,	
                T_MOUVEMENTS.CODE_MAGASIN AS CODE_MAGASIN,	
                T_MOUVEMENTS.COMPTE_ECART AS COMPTE_ECART,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT,	
                T_MOUVEMENTS.MOTIF AS MOTIF,	
                T_MOUVEMENTS.MONTANT AS MONTANT,	
                T_MOUVEMENTS.PRIX AS PRIX,	
                T_MOUVEMENTS.MONTANT_ECART AS MONTANT_ECART,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT
            FROM 
                T_PRODUITS,	
                T_ARTICLES,	
                T_MOUVEMENTS,	
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_MOUVEMENTS.COMPTE_ECART
                AND		T_ARTICLES.CODE_ARTICLE = T_MOUVEMENTS.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                {CODE_BLOCK_1}
        '''

        try:
            kwargs = {
                'Param_origine': args[0],
                'Paramtype_produit': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['CODE_BLOCK_1'] = '''AND
                (
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )'''
        kwargs['OPTIONAL_ARG_1'] = 'T_MOUVEMENTS.ORIGINE = {Param_origine}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_MOUVEMENTS.TYPE_PRODUIT = {Paramtype_produit}'

        if kwargs['Param_origine'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
            kwargs['OPTIONAL_ARG_2'] = kwargs['OPTIONAL_ARG_2'][4:]
        
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Paramtype_produit'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        kwargs['CODE_BLOCK_1'] = '' if kwargs['OPTIONAL_ARG_1'] == '' and kwargs['OPTIONAL_ARG_2'] == '' else kwargs['CODE_BLOCK_1']

        return query.format(**kwargs).format(**kwargs).format(**kwargs)

    
    def Req_dt_prelevement(self, args): #Done
        query = '''
            SELECT 
                T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT AS ID_PRELEVEMENT,	
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_DT_PRELEVEMENT_COND.SUSP_VENTE AS SUSP_VENTE,	
                T_DT_PRELEVEMENT_COND.SOLDE_STD AS SOLDE_STD,	
                T_DT_PRELEVEMENT_COND.SOLDE_AGR AS SOLDE_AGR,	
                T_DT_PRELEVEMENT_COND.SOLDE_PRM AS SOLDE_PRM,	
                T_DT_PRELEVEMENT_COND.SOLDE_PAG AS SOLDE_PAG,	
                T_DT_PRELEVEMENT_COND.SOLDE_PUHT AS SOLDE_PUHT,	
                T_DT_PRELEVEMENT_COND.SOLDE_CS1 AS SOLDE_CS1,	
                T_DT_PRELEVEMENT_COND.DECON_STD AS DECON_STD,	
                T_DT_PRELEVEMENT_COND.DECON_AGR AS DECON_AGR,	
                T_DT_PRELEVEMENT_COND.DECON_PRM AS DECON_PRM,	
                T_DT_PRELEVEMENT_COND.DECON_PAG AS DECON_PAG,	
                T_DT_PRELEVEMENT_COND.DECON_PUHT AS DECON_PUHT,	
                T_DT_PRELEVEMENT_COND.DECON_CS1 AS DECON_CS1,	
                T_DT_PRELEVEMENT_COND.PRIME_STD AS PRIME_STD,	
                T_DT_PRELEVEMENT_COND.PRIME_AGR AS PRIME_AGR,	
                T_DT_PRELEVEMENT_COND.PRIME_PRM AS PRIME_PRM,	
                T_DT_PRELEVEMENT_COND.PRIME_PAG AS PRIME_PAG,	
                T_DT_PRELEVEMENT_COND.PRIME_PUHT AS PRIME_PUHT,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_FONCTION.NOM_FONCTION AS NOM_FONCTION,	
                T_DT_PRELEVEMENT_COND.SOLDE_EURO AS SOLDE_EURO,	
                T_DT_PRELEVEMENT_COND.SOLDE_CBL AS SOLDE_CBL,	
                T_DT_PRELEVEMENT_COND.DECON_EURO AS DECON_EURO,	
                T_DT_PRELEVEMENT_COND.DECON_CBL AS DECON_CBL
            FROM 
                T_FONCTION,	
                T_OPERATEUR,	
                T_DT_PRELEVEMENT_COND
            WHERE 
                T_FONCTION.CODE_FONCTION = T_OPERATEUR.FONCTION
                AND		T_OPERATEUR.CODE_OPERATEUR = T_DT_PRELEVEMENT_COND.CODE_OPERATEUR
                AND
                (
                    T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT = {Param_id_prelevement}
                )
        '''

        try:
            kwargs = {
                'Param_id_prelevement': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_prelevement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_dt_promo_tranche_article(self, args): #Done
        query = '''
            SELECT 
                T_DT_PROMO_TRANCHE.ID_PROMO AS ID_PROMO,	
                T_DT_PROMO_TRANCHE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_DT_PROMO_TRANCHE.TRANCHE AS TRANCHE,	
                T_DT_PROMO_TRANCHE.GRATUIT AS GRATUIT,	
                T_DT_PROMO_TRANCHE.TX_GRATUIT AS TX_GRATUIT
            FROM 
                T_DT_PROMO_TRANCHE
            WHERE 
                T_DT_PROMO_TRANCHE.ID_PROMO = {Param_ID_PROMO}
                AND	T_DT_PROMO_TRANCHE.CODE_ARTICLE = {Param_CODE_ARTICLE}
            ORDER BY 
                TRANCHE DESC
        '''

        try:
            kwargs = {
                'Param_ID_PROMO': args[0],
                'Param_CODE_ARTICLE': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_dt_reclamation(self, args): #Done
        query = '''
            SELECT 
                T_DT_RECLAMATION.ID_RECLAMATION AS ID_RECLAMATION,	
                T_MOTIF_RECLAMATION.LIBELLE AS LIBELLE
            FROM 
                T_MOTIF_RECLAMATION,	
                T_DT_RECLAMATION
            WHERE 
                T_MOTIF_RECLAMATION.ID_MOTIF = T_DT_RECLAMATION.ID_MOTIF
                AND
                (
                    T_DT_RECLAMATION.ID_RECLAMATION = {Param_id_reclamation}
                )
        '''

        try:
            kwargs = {
                'Param_id_reclamation': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_reclamation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_dt_rendus(self, args): #Done
        query = '''
            SELECT 
                T_LIGNE_RETOUR_RENDUS.ID_RETOUR AS ID_RETOUR,	
                T_LIGNE_RETOUR_RENDUS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_LIGNE_RETOUR_RENDUS.CATEGORIE AS CATEGORIE,	
                T_LIGNE_RETOUR_RENDUS.QTE_AGCE AS QTE_AGCE,	
                T_LIGNE_RETOUR_RENDUS.COMPT1 AS COMPT1,	
                T_LIGNE_RETOUR_RENDUS.COMPT2 AS COMPT2,	
                T_LIGNE_RETOUR_RENDUS.ECART AS ECART,	
                ( T_LIGNE_RETOUR_RENDUS.PRIX * T_LIGNE_RETOUR_RENDUS.ECART )  AS VAL_ECART,	
                T_ARTICLES.LIBELLE AS LIBELLE
            FROM 
                T_ARTICLES,	
                T_LIGNE_RETOUR_RENDUS
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_LIGNE_RETOUR_RENDUS.CODE_ARTICLE
                AND
                (
                    T_LIGNE_RETOUR_RENDUS.ID_RETOUR = {Param_id_retour}
                )
        '''

        try:
            kwargs = {
                'Param_id_retour': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_retour'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_dt_retour(self, args): #Done
        query = '''
            SELECT 
                T_LIGNE_RETOUR_CAISSERIE.ID_RETOUR AS ID_RETOUR,	
                T_LIGNE_RETOUR_CAISSERIE.CODE_CP AS CODE_CP,	
                T_LIGNE_RETOUR_CAISSERIE.QTE_AGCE AS QTE_AGCE,	
                T_LIGNE_RETOUR_CAISSERIE.COMPT1 AS COMPT1,	
                T_LIGNE_RETOUR_CAISSERIE.COMPT2 AS COMPT2,	
                T_LIGNE_RETOUR_CAISSERIE.ECART AS ECART,	
                T_CAISSES_PALETTES.NOM_TYPE AS NOM_TYPE,	
                T_CAISSES_PALETTES.RANG AS RANG
            FROM 
                T_CAISSES_PALETTES,	
                T_LIGNE_RETOUR_CAISSERIE
            WHERE 
                T_CAISSES_PALETTES.CODE_TYPE = T_LIGNE_RETOUR_CAISSERIE.CODE_CP
                AND
                (
                    T_LIGNE_RETOUR_CAISSERIE.ID_RETOUR = {Param_id_retour}
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_id_retour': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_retour'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ecarts(self, args): #Done
        query = '''
            SELECT 
                T_RETOURS_USINE.ID_RETOUR AS ID_RETOUR,	
                T_RETOURS_USINE.DATE_RETOUR AS DATE_RETOUR,	
                T_RETOURS_USINE.CATEGORIE AS CATEGORIE,	
                T_RETOURS_USINE.COMPTE_ECART AS COMPTE_ECART,	
                T_RETOURS_USINE.VALID AS VALID,	
                T_RETOURS_USINE.REF_OPERATION AS REF_OPERATION,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATIONS.COMPTE_ECART AS CMPTE_ECART,	
                CONT.NOM_OPERATEUR AS CONTROLEUR,	
                SUM(( T_LIGNE_RETOUR_RENDUS.ECART * T_LIGNE_RETOUR_RENDUS.PRIX ) ) AS la_somme_ECART
            FROM 
                (
                    (
                        (
                            T_RETOURS_USINE
                            INNER JOIN
                            T_LIGNE_RETOUR_RENDUS
                            ON T_RETOURS_USINE.ID_RETOUR = T_LIGNE_RETOUR_RENDUS.ID_RETOUR
                        )
                        LEFT OUTER JOIN
                        T_OPERATIONS
                        ON T_OPERATIONS.CODE_OPERATION = T_RETOURS_USINE.ID_RETOUR
                    )
                    LEFT OUTER JOIN
                    T_OPERATEUR
                    ON T_RETOURS_USINE.COMPTE_ECART = T_OPERATEUR.CODE_OPERATEUR
                )
                LEFT OUTER JOIN
                T_OPERATEUR CONT
                ON CONT.CODE_OPERATEUR = T_OPERATIONS.COMPTE_ECART
            WHERE 
                (
                {OPTIONAL_ARG_1}
                {OPTIONAL_ARG_2}
                T_RETOURS_USINE.CATEGORIE <> {Param_diff_categorie}
            )
            GROUP BY 
                T_RETOURS_USINE.ID_RETOUR,	
                T_RETOURS_USINE.DATE_RETOUR,	
                T_RETOURS_USINE.CATEGORIE,	
                T_RETOURS_USINE.COMPTE_ECART,	
                T_RETOURS_USINE.VALID,	
                T_RETOURS_USINE.REF_OPERATION,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_OPERATIONS.COMPTE_ECART,	
                CONT.NOM_OPERATEUR
            HAVING 
                SUM(( T_LIGNE_RETOUR_RENDUS.ECART * T_LIGNE_RETOUR_RENDUS.PRIX ) ) < 0
            OR	SUM(( T_LIGNE_RETOUR_RENDUS.ECART * T_LIGNE_RETOUR_RENDUS.PRIX ) ) > 0
            ORDER BY 
                DATE_RETOUR DESC
        '''

        try:
            kwargs = {
                'Param_valid': args[0],
                'Param_egal_categorie': args[1],
                'Param_diff_categorie': args[2]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_diff_categorie'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_RETOURS_USINE.VALID = {Param_valid} AND'
        kwargs['OPTIONAL_ARG_2'] = 'T_RETOURS_USINE.CATEGORIE = {Param_egal_categorie} AND'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_valid'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_egal_categorie'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ecarts_caisserie(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_RETOURS_USINE.ID_RETOUR AS ID_RETOUR,	
                T_RETOURS_USINE.DATE_RETOUR AS DATE_RETOUR,	
                T_RETOURS_USINE.CATEGORIE AS CATEGORIE,	
                T_RETOURS_USINE.COMPTE_ECART AS COMPTE_ECART,	
                T_RETOURS_USINE.VALID AS VALID,	
                T_RETOURS_USINE.REF_OPERATION AS REF_OPERATION,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATIONS.COMPTE_ECART AS CMPTE_ECART,	
                CONT.NOM_OPERATEUR AS CONTROLEUR
            FROM 
                (
                    (
                        (
                            T_RETOURS_USINE
                            INNER JOIN
                            T_LIGNE_RETOUR_CAISSERIE
                            ON T_RETOURS_USINE.ID_RETOUR = T_LIGNE_RETOUR_CAISSERIE.ID_RETOUR
                        )
                        LEFT OUTER JOIN
                        T_OPERATIONS
                        ON T_OPERATIONS.CODE_OPERATION = T_RETOURS_USINE.ID_RETOUR
                    )
                    LEFT OUTER JOIN
                    T_OPERATEUR
                    ON T_RETOURS_USINE.COMPTE_ECART = T_OPERATEUR.CODE_OPERATEUR
                )
                LEFT OUTER JOIN
                T_OPERATEUR CONT
                ON CONT.CODE_OPERATEUR = T_OPERATIONS.COMPTE_ECART
            WHERE 
                (
                {OPTIONAL_ARG_1}
                T_RETOURS_USINE.CATEGORIE = {Param_egal_categorie}
                {OPTIONAL_ARG_2}
                AND	T_LIGNE_RETOUR_CAISSERIE.ECART <> 0
            )
            ORDER BY 
                DATE_RETOUR DESC
        '''

        try:
            kwargs = {
                'Param_valid': args[0],
                'Param_egal_categorie': args[1],
                'Param_diff_categorie': args[2]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_egal_categorie'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_RETOURS_USINE.VALID = {Param_valid} AND'
        kwargs['OPTIONAL_ARG_2'] = 'AND T_RETOURS_USINE.CATEGORIE <> {Param_diff_categorie}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_valid'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_diff_categorie'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ecarts_inventaire_par_magasin(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.CODE_MAGASIN AS CODE_MAGASIN,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_ECART) AS la_somme_QTE_ECART,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT
            FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                AND	T_MOUVEMENTS.TYPE_MOUVEMENT = 'I'
            GROUP BY 
                T_MOUVEMENTS.CODE_MAGASIN,	
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT
        '''
        
        try:
            kwargs = {
                'Param_date_mvt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_etat_borderau_valeurs(self, args): #Done
        query = '''
            SELECT 
                T_DT_BORDEREAU.ID_BORDEREAU AS ID_BORDEREAU,	
                T_DT_BORDEREAU.NUM_DECOMPTE AS NUM_DECOMPTE,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_DECOMPTE.REFERENCE AS REFERENCE,	
                T_DT_DECOMPTE.LE_TIRE AS LE_TIRE,	
                T_BORDEREAU_VALEURS.NUM_BORDEAU_BANQUE AS NUM_BORDEAU_BANQUE,	
                T_BORDEREAU_VALEURS.DATE_HEURE_SAISIE AS DATE_HEURE_SAISIE
            FROM 
                T_BORDEREAU_VALEURS,	
                T_DECOMPTE,	
                T_DT_DECOMPTE,	
                T_DT_BORDEREAU
            WHERE 
                T_BORDEREAU_VALEURS.ID_BORDEREAU = T_DT_BORDEREAU.ID_BORDEREAU
                AND		T_DECOMPTE.NUM_DECOMPTE = T_DT_BORDEREAU.NUM_DECOMPTE
                AND		T_DECOMPTE.NUM_DECOMPTE = T_DT_DECOMPTE.NUM_DECOMPTE
                AND
                (
                    T_DT_BORDEREAU.ID_BORDEREAU = {Param_id_bordereau}
                )
        '''
        
        try:
            kwargs = {
                'Param_id_bordereau': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_bordereau'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def req_etat_chargement(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.VALID AS VALID,	
                T_CHARGEMENT.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_CHARGEMENT.code_chauffeur AS code_chauffeur,	
                T_CHARGEMENT.AIDE_VENDEUR1 AS AIDE_VENDEUR1,	
                T_CHARGEMENT.AIDE_VENDEUR2 AS AIDE_VENDEUR2,	
                T_CHARGEMENT.vehicule AS vehicule
            FROM 
                T_CHARGEMENT
            WHERE 
                T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_etat_journee(self, args): #Done
        query = '''
            SELECT 
                T_JOURNEE.DATE_JOURNEE AS DATE_JOURNEE,	
                T_JOURNEE.CODE_AGCE AS CODE_AGCE,	
                T_JOURNEE.STOCK AS STOCK,	
                T_JOURNEE.SOLDE_EMB AS SOLDE_EMB,	
                T_JOURNEE.CLOTURE AS CLOTURE,	
                T_JOURNEE.JOURNEE_TEMP AS JOURNEE_TEMP,	
                T_JOURNEE.SOLDE_CAISSERIE AS SOLDE_CAISSERIE
            FROM 
                T_JOURNEE
            WHERE 
                T_JOURNEE.DATE_JOURNEE = '{Param_date_journee}'
                AND T_JOURNEE.CODE_AGCE = {code_agce}
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0],
                'code_agce': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_etat_synchro(self, args): #Done
        query = '''
            SELECT 
                T_SYNCHRO.ID_JOB AS ID_JOB,	
                T_SYNCHRO.OPERATION AS OPERATION,	
                T_SYNCHRO.SOUS_OPERATION AS SOUS_OPERATION,	
                T_SYNCHRO.ETAT AS ETAT,	
                T_SYNCHRO.ID_OPERATION AS ID_OPERATION
            FROM 
                T_SYNCHRO
            WHERE 
                T_SYNCHRO.OPERATION = {Param_OPERATION}
                AND	T_SYNCHRO.SOUS_OPERATION = {Param_SOUS_OPERATION}
                AND	T_SYNCHRO.ID_OPERATION = {Param_ID_OPERATION}
        '''

        try:
            kwargs = {
                'Param_OPERATION': args[0],
                'Param_SOUS_OPERATION': args[1],
                'Param_ID_OPERATION': args[2]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_etat_validation_remise(self, args): #Done
        query = '''
            SELECT 
                T_REMISE_CLIENT.Date_Debut AS Date_Debut,	
                T_REMISE_CLIENT.STATUT AS STATUT
            FROM 
                T_REMISE_CLIENT
            WHERE 
                T_REMISE_CLIENT.Date_Debut = '{Param_DATE_DEBUT}'
        '''
        
        try:
            kwargs = {
                'Param_DATE_DEBUT': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_DATE_DEBUT'] = self.validateDate(kwargs['Param_DATE_DEBUT'])

        if kwargs['Param_DATE_DEBUT'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_export_invendu_rendus(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE) AS INVENDU,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_POINTE) AS TOTAL_RENDUS,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM) AS RENDUS_COM,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_US) AS RENDUS_US,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG) AS RENDUS_AG,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP) AS RENDUS_SP
            FROM 
                T_ARTICLES
                LEFT OUTER JOIN
                T_PRODUITS_CHARGEE
                ON T_ARTICLES.CODE_ARTICLE = T_PRODUITS_CHARGEE.CODE_ARTICLE
            WHERE 
                (
                T_ARTICLES.ACTIF = 1
                {OPTIONAL_ARG_1}
            )
            GROUP BY 
                T_ARTICLES.LIBELLE_COURT,	
                T_ARTICLES.RANG,	
                T_ARTICLES.CODE_ARTICLE
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_date': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])
        kwargs['OPTIONAL_ARG_1'] = '''AND T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date}' '''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_facture_periode(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                MIN(T_ARTICLES.RANG) AS le_minimum_RANG,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_DT_FACTURE.PRIX AS PRIX,	
                T_ARTICLES.CODE_BARRE AS CODE_BARRE,	
                T_ARTICLES.LIBELLE AS NOM_PRODUIT,	
                T_ARTICLES.TVA AS TVA,	
                SUM(( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX ) ) AS MT,	
                SUM(( ( ( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX ) * T_DT_FACTURE.TX_GRATUIT ) /  100) ) AS REMISE_SF,	
                SUM(T_DT_FACTURE.QTE_VENTE) AS la_somme_QTE_VENTE,	
                SUM(T_DT_FACTURE.QTE_PERTE) AS la_somme_QTE_PERTE,	
                SUM(T_DT_FACTURE.QTE_PROMO) AS la_somme_QTE_PROMO,	
                SUM(T_DT_FACTURE.QTE_REMISE) AS la_somme_QTE_REMISE,	
                SUM(( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PROMO ) - T_DT_FACTURE.QTE_PERTE ) ) AS la_somme_QTE
            FROM 
                T_FACTURE,	
                T_DT_FACTURE,	
                T_CLIENTS,	
                T_ARTICLES,	
                T_PRODUITS
            WHERE 
                T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.CODE_CLIENT IN ({Param_code_client}) 
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_FACTURE.CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT,	
                T_DT_FACTURE.PRIX,	
                T_ARTICLES.LIBELLE,	
                T_ARTICLES.TVA,	
                T_ARTICLES.CODE_BARRE
            ORDER BY 
                CODE_CLIENT ASC,	
                le_minimum_RANG ASC
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_famille_gamme(self, args): #Done
        query = '''
            SELECT 
                T_FAMILLE.CODE_FAMILLE AS CODE_FAMILLE,	
                T_FAMILLE.NOM_FAMILLE AS NOM_FAMILLE,	
                T_FAMILLE.CODE_GAMME AS CODE_GAMME
            FROM 
                T_FAMILLE
            WHERE 
                T_FAMILLE.CODE_GAMME = {Param_code_gamme}
        '''
        
        try:
            kwargs = {
                'Param_code_gamme': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_gamme'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_get_id_det_mission(self, args): #Done
        query = '''
            SELECT 
                T_Det_Mission.Id_Det_Mission AS Id_Det_Mission,	
                T_Det_Mission.Id_Trajet AS Id_Trajet,	
                T_Det_Mission.Id_Ordre_Mission AS Id_Ordre_Mission,	
                T_Trajet_Destintion.Id_Destination AS Id_Destination,	
                T_Trajet_Destintion.Id_Trajet AS Id_Trajet_T_,	
                T_Trajet_Destintion.Depart AS Depart
            FROM 
                T_Trajet_Destintion,	
                T_Det_Mission
            WHERE 
                T_Det_Mission.Id_Trajet = T_Trajet_Destintion.Id_Trajet
                AND
                (
                    T_Trajet_Destintion.Depart = 0
                    AND	T_Trajet_Destintion.Id_Destination = {Param_id_dest}
                    AND	T_Det_Mission.Id_Ordre_Mission = {Param_id_ordre_mission}
                )
        '''

        try:
            kwargs = {
                'Param_id_dest': args[0],
                'Param_id_ordre_mission': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def REQ_Get_MissionBL_By_Ordre(self, args): #Done
        query = '''
            SELECT 
                MAX(T_Mission_BL.Id_Det) AS MAX_Id_Det
            FROM 
                T_Mission_BL
        '''
        return query

    
    def Req_get_prevendeur_date(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDE_CLIENT.DATE_COMMANDE AS DATE_COMMANDE,	
                T_COMMANDE_CLIENT.CODE_PREVENDEUR AS CODE_PREVENDEUR
            FROM 
                T_COMMANDE_CLIENT
            WHERE 
                T_COMMANDE_CLIENT.DATE_COMMANDE = '{Param_DATE_COMMANDE}'
        '''

        try:
            kwargs = {
                'Param_DATE_COMMANDE': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_DATE_COMMANDE'] = self.validateDate(kwargs['Param_DATE_COMMANDE'])

        if kwargs['Param_DATE_COMMANDE'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def REQ_GetEnseigne(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_GROUP_CLIENTS.ID_GP_CLIENT AS ID_GP_CLIENT,	
                T_GROUP_CLIENTS.NOM_GROUP AS NOM_GROUP
            FROM 
                T_GROUP_CLIENTS,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.GROUP_CLIENT = T_GROUP_CLIENTS.ID_GP_CLIENT
        '''
        return query

    
    def Req_info_bl_mission(self, args): #Done
        query = '''
            SELECT 
                T_Mission_BL.Id_Det_Mission AS Id_Det_Mission,	
                T_Mission_BL.Num_BL AS Num_BL,	
                T_Mission_BL.Id_Det AS Id_Det,	
                T_Mission_BL.Id_Type_Livraison AS Id_Type_Livraison,	
                T_Mission_BL.BL_Valider AS BL_Valider
            FROM 
                T_Mission_BL
            WHERE 
                T_Mission_BL.Num_BL = {Param_num_bl}
        '''

        try:
            kwargs = {
                'Param_num_bl': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_num_bl'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_info_caisse(self, args): #Done
        query = '''
            SELECT 
                T_CAISSE.CODE_CAISSE AS CODE_CAISSE,	
                T_CAISSE.VALEUR_ACT AS VALEUR_ACT
            FROM 
                T_CAISSE
            WHERE 
                T_CAISSE.CODE_CAISSE = {Param_code_caisse}
        '''

        try:
            kwargs = {
                'Param_code_caisse': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_caisse'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_info_chargement(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE AS CODE_TOURNEE,	
                T_CHARGEMENT.vehicule AS vehicule,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_CHARGEMENT.code_chauffeur AS code_chauffeur,	
                T_CHARGEMENT.AIDE_VENDEUR1 AS AIDE_VENDEUR1,	
                T_CHARGEMENT.AIDE_VENDEUR2 AS AIDE_VENDEUR2,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_TOURNEES.NOM_TOURNEE AS NOM_TOURNEE,	
                T_CHARGEMENT.MONTANT_A_VERSER AS MONTANT_A_VERSER,	
                T_SECTEUR.CAT_SECTEUR AS CAT_SECTEUR,	
                T_SECTEUR.RANG AS RANG,	
                T_CHARGEMENT.VALID AS VALID
            FROM 
                T_SECTEUR,	
                T_TOURNEES,	
                T_CHARGEMENT,	
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND		T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_SECTEUR.code_secteur = T_TOURNEES.code_secteur
                AND
                (
                    T_CHARGEMENT.CODE_CHARGEMENT = {Param_code_chargement}
                )
        '''

        try:
            kwargs = {
                'Param_code_chargement': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_chargement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_info_client(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_CLIENTS.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_CLIENTS.SOLDE_C_STD AS SOLDE_C_STD,	
                T_CLIENTS.SOLDE_P_AG AS SOLDE_P_AG,	
                T_CLIENTS.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_CLIENTS.SOLDE_C_AG AS SOLDE_C_AG,	
                T_CLIENTS.SOLDE_C_PR AS SOLDE_C_PR,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_CLIENTS.GROUP_CLIENT AS GROUP_CLIENT
            FROM 
                T_SOUS_SECTEUR,	
                T_CLIENTS
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_CLIENTS.CODE_CLIENT = {Param_code_client}
                )
        '''

        try:
            kwargs = {
                'Param_code_client': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_client'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_info_journee(self, args): #Done
        query = '''
            SELECT 
                T_JOURNEE.DATE_JOURNEE AS DATE_JOURNEE,	
                T_JOURNEE.CODE_AGCE AS CODE_AGCE,	
                T_JOURNEE.STOCK AS STOCK,	
                T_JOURNEE.SOLDE_EMB AS SOLDE_EMB,	
                T_JOURNEE.CLOTURE AS CLOTURE,	
                T_JOURNEE.JOURNEE_TEMP AS JOURNEE_TEMP,	
                T_JOURNEE.SOLDE_CAISSE AS SOLDE_CAISSE,	
                T_JOURNEE.AS_C_STD AS AS_C_STD,	
                T_JOURNEE.AS_P_AG AS AS_P_AG,	
                T_JOURNEE.AS_P_UHT AS AS_P_UHT,	
                T_JOURNEE.AS_C_AG AS AS_C_AG,	
                T_JOURNEE.AS_C_PR AS AS_C_PR,	
                T_JOURNEE.NS_C_STD AS NS_C_STD,	
                T_JOURNEE.NS_P_AG AS NS_P_AG,	
                T_JOURNEE.NS_P_UHT AS NS_P_UHT,	
                T_JOURNEE.NS_C_AG AS NS_C_AG,	
                T_JOURNEE.NS_C_PR AS NS_C_PR,	
                T_JOURNEE.SOLDE_CAISSERIE AS SOLDE_CAISSERIE,	
                T_JOURNEE.AS_P_EURO AS AS_P_EURO,	
                T_JOURNEE.AS_CS_BLC AS AS_CS_BLC,	
                T_JOURNEE.NS_PAL_EURO AS NS_PAL_EURO,	
                T_JOURNEE.NS_CS_BLC AS NS_CS_BLC,	
                T_JOURNEE.AS_CS1 AS AS_CS1,	
                T_JOURNEE.AS_CS2 AS AS_CS2,	
                T_JOURNEE.NV_CS1 AS NV_CS1,	
                T_JOURNEE.NV_CS2 AS NV_CS2
            FROM 
                T_JOURNEE
            WHERE 
                T_JOURNEE.DATE_JOURNEE = '{Param_date_journee}'
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_info_mission(self, args): #Done
        query = '''
            SELECT 
                T_Ordre_Mission_Agence.Id_Ordre_Mission AS Id_Ordre_Mission,	
                T_Ordre_Mission_Agence.Matricule_Vehicule AS Matricule_Vehicule,	
                T_Ordre_Mission_Agence.Date_Ordre AS Date_Ordre,	
                T_Ordre_Mission_Agence.Matricule_Semi AS Matricule_Semi,	
                T_Ordre_Mission_Agence.Mission_Fini AS Mission_Fini,	
                T_Ordre_Mission_Agence.Chauffeurs_Mission AS Chauffeurs_Mission,	
                T_Ordre_Mission_Agence.Scelle AS Scelle,	
                T_Ordre_Mission_Agence.Nom_Transporteur AS Nom_Transporteur,	
                T_Ordre_Mission_Agence.Type_Transport AS Type_Transport
            FROM 
                T_Ordre_Mission_Agence
            WHERE 
                T_Ordre_Mission_Agence.Id_Ordre_Mission = {Param_id_mission}
        '''

        try:
            kwargs = {
                'Param_id_mission': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_mission'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_info_operateur(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_OPERATEUR.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_OPERATEUR.SOLDE_C_STD AS SOLDE_C_STD,	
                T_OPERATEUR.SOLDE_P_AG AS SOLDE_P_AG,	
                T_OPERATEUR.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_OPERATEUR.SOLDE_C_AG AS SOLDE_C_AG,	
                T_OPERATEUR.SOLDE_C_PR AS SOLDE_C_PR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR
            FROM 
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = {Param_code_operateur}
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_operateur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_info_tournee_client(self, args): #Done
        query = '''
            SELECT 
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_ITINERAIRES.CODE_TOURNEE AS CODE_TOURNEE,	
                T_ITINERAIRES.CODE_CLIENT AS CODE_CLIENT,	
                T_TOURNEES.NOM_TOURNEE AS NOM_TOURNEE
            FROM 
                T_TOURNEES,	
                T_ITINERAIRES,	
                T_CLIENTS,	
                T_SOUS_SECTEUR
            WHERE 
                T_TOURNEES.CODE_TOURNEE = T_ITINERAIRES.CODE_TOURNEE
                AND		T_ITINERAIRES.CODE_CLIENT = T_CLIENTS.CODE_CLIENT
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
                )
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_info_trajet(self, args): #Done
        query = '''
            SELECT 
                T_Hist_Trajet.Id_Ordre_Mission AS Id_Ordre_Mission,	
                T_Hist_Trajet.Lieu_Depart AS Lieu_Depart,	
                T_Hist_Trajet.Date_Heure_depart AS Date_Heure_depart,	
                T_Hist_Trajet.Scelle1 AS Scelle1,	
                T_Hist_Trajet.Lieu_Arrivee AS Lieu_Arrivee,	
                T_Hist_Trajet.Date_Heure_Arrivee AS Date_Heure_Arrivee,	
                T_Hist_Trajet.Scelle2 AS Scelle2,	
                T_Hist_Trajet.INDEX1 AS INDEX1,	
                T_Hist_Trajet.TEMP1 AS TEMP1,	
                T_Hist_Trajet.TEMP2 AS TEMP2,	
                T_Hist_Trajet.Date_effet_Depart AS Date_effet_Depart,	
                T_Hist_Trajet.Date_effet_Arrivee AS Date_effet_Arrivee
            FROM 
                T_Hist_Trajet
            WHERE 
                T_Hist_Trajet.Id_Ordre_Mission = {Param_id_mission}
                {OPTIONAL_ARG_1}
                AND	T_Hist_Trajet.Lieu_Depart = {Param_depart}
        '''

        try:
            kwargs = {
                'Param_id_mission': args[0],
                'Param_arrivee': args[1],
                'Param_depart': args[2]
            }
        except IndexError as e:
            return e
        
        for key in ('Param_id_mission', 'Param_depart'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_Hist_Trajet.Lieu_Arrivee = {Param_arrivee}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_arrivee'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_itineraire(self, args): #Done
        query = '''
            SELECT 
                T_ITINERAIRES.CODE_TOURNEE AS CODE_TOURNEE,	
                T_ITINERAIRES.CODE_CLIENT AS CODE_CLIENT,	
                T_ITINERAIRES.RANG AS RANG,	
                T_ITINERAIRES.RANG_PREVENTE AS RANG_PREVENTE
            FROM 
                T_ITINERAIRES
            WHERE 
                T_ITINERAIRES.CODE_TOURNEE = {Param_code_tournee}
        '''

        try:
            kwargs = {
                'Param_code_tournee': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_tournee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_jours_non_clos(self, args): #Done
        query = '''
            SELECT 
                COUNT(T_JOURNEE.CLOTURE) AS Comptage_1,	
                T_JOURNEE.CODE_AGCE AS CODE_AGCE
            FROM 
                T_JOURNEE
            WHERE 
                T_JOURNEE.CLOTURE = 0
                {OPTIONAL_ARG_1}
            GROUP BY 
                T_JOURNEE.CODE_AGCE
        '''

        try:
            kwargs = {
                'Param_code_agence': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_JOURNEE.CODE_AGCE = {Param_code_agence}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_agence'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_lignes_commande(self, args): #Done
        query = '''
            SELECT 
                T_LIGNE_COMMANDE.ID_COMMANDE AS ID_COMMANDE,	
                T_LIGNE_COMMANDE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_LIGNE_COMMANDE.QTE_COMMANDE AS QTE_COMMANDE,	
                T_LIGNE_COMMANDE.STOCK AS STOCK,	
                T_LIGNE_COMMANDE.QTE_LIVREE AS QTE_LIVREE,	
                T_LIGNE_COMMANDE.QTE_PERTE AS QTE_PERTE,	
                T_LIGNE_COMMANDE.QTE_PROMO AS QTE_PROMO,	
                T_LIGNE_COMMANDE.TX_GRATUIT AS TX_GRATUIT,	
                ( ( T_LIGNE_COMMANDE.QTE_LIVREE - T_LIGNE_COMMANDE.QTE_PERTE ) - T_LIGNE_COMMANDE.QTE_PROMO )  AS QTE_MVT,	
                ( ( ( T_LIGNE_COMMANDE.QTE_LIVREE - T_LIGNE_COMMANDE.QTE_PERTE ) - T_LIGNE_COMMANDE.QTE_PROMO ) * T_LIGNE_COMMANDE.PRIX )  AS CA,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.TVA AS TVA,	
                T_LIGNE_COMMANDE.PRIX AS PRIX,	
                T_LIGNE_COMMANDE.REMISE AS REMISE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT
            FROM 
                T_ARTICLES,	
                T_LIGNE_COMMANDE
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_LIGNE_COMMANDE.CODE_ARTICLE
                AND
                (
                    T_LIGNE_COMMANDE.ID_COMMANDE = {Param_id_commande}
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_id_commande': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_commande'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_livraison_articles(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_LIVREES.CODE_CLIENT AS CODE_CLIENT,	
                T_PRODUITS_LIVREES.QTE_IMPORTE AS QTE_IMPORTE,	
                T_PRODUITS_LIVREES.QTE_CHARGEE AS QTE_CHARGEE,	
                T_PRODUITS_LIVREES.QTE_CAISSE AS QTE_CAISSE,	
                T_PRODUITS_LIVREES.QTE_PAL AS QTE_PAL,	
                T_PRODUITS_LIVREES.QTE_COMMANDE AS QTE_COMMANDE,	
                T_PRODUITS_LIVREES.MONTANT AS MONTANT,	
                T_PRODUITS_LIVREES.TYPE_CLIENT AS TYPE_CLIENT,	
                T_PRODUITS_LIVREES.code_vendeur AS code_vendeur,	
                T_PRODUITS_LIVREES.PRIX AS PRIX,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_PRODUITS_LIVREES.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.STATUT AS STATUT
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_PRODUITS_LIVREES.CODE_CLIENT = {Param_code_client}
                    AND	T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_PRODUITS_LIVREES.TYPE_MVT = 'L'
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
        '''

        try:
            kwargs = {
                'Param_code_article': args[0],
                'Param_code_client': args[1],
                'Param_date_livraison': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])
        
        for key in ('Param_code_client', 'Param_date_livraison'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_PRODUITS_LIVREES.CODE_ARTICLE = {Param_code_article} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_article'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_livraison_cond(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS_CAISSERIE.CODE_CP AS CODE_CP,	
                T_MOUVEMENTS_CAISSERIE.ORIGINE AS ORIGINE,	
                T_MOUVEMENTS_CAISSERIE.QTE_THEORIQUE AS QTE_THEORIQUE,	
                T_MOUVEMENTS_CAISSERIE.QTE_REEL AS QTE_REEL,	
                T_MOUVEMENTS_CAISSERIE.QTE_MOUVEMENT AS QTE_MOUVEMENT
            FROM 
                T_MOUVEMENTS_CAISSERIE
            WHERE 
                {OPTIONAL_ARG_1}
                T_MOUVEMENTS_CAISSERIE.ORIGINE = {Param_origine}
        '''

        try:
            kwargs = {
                'Param_code_cp': args[0],
                'Param_origine': args[1]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_origine'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_MOUVEMENTS_CAISSERIE.CODE_CP = {Param_code_cp} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_cp'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_livraison_conditionnement(self, args): #Done
        query = '''
            SELECT 
                T_COND_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                T_COND_LIVRAISON.CODE_MAGASIN AS CODE_MAGASIN,	
                T_COND_LIVRAISON.QTE_IMPORTE AS QTE_IMPORTE,	
                T_COND_LIVRAISON.QTE_CHARGEE AS QTE_CHARGEE,	
                T_COND_LIVRAISON.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COND_LIVRAISON.TYPE_MVT AS TYPE_MVT
            FROM 
                T_LIVRAISON,	
                T_COND_LIVRAISON
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_COND_LIVRAISON.NUM_LIVRAISON
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_COND_LIVRAISON.CODE_CLIENT = {Param_code_client}
                    AND	T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_COND_LIVRAISON.TYPE_MVT = 'L'
                )
        '''

        try:
            kwargs = {
                'Param_code_cp': args[0],
                'Param_code_client': args[1],
                'Param_date_livraison': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in ('Param_code_client', 'Param_date_livraison'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_MOUVEMENTS_CAISSERIE.CODE_CP = {Param_code_cp} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_cp'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_livraison_global_client(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                T_LIVRAISON.code_vendeur AS code_vendeur,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.RANG AS RANG,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.RANG AS RANG_T_
            FROM 
                T_PRODUITS_LIVREES,	
                T_LIVRAISON,	
                T_CLIENTS,	
                T_ARTICLES
            WHERE 
                T_PRODUITS_LIVREES.CODE_ARTICLE = T_ARTICLES.CODE_ARTICLE
                AND		T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND		T_PRODUITS_LIVREES.NUM_LIVRAISON = T_LIVRAISON.NUM_LIVRAISON
                AND
                (
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                    T_LIVRAISON.STATUT <> 'A'
                )
            GROUP BY 
                T_LIVRAISON.DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT,	
                T_LIVRAISON.DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT,	
                T_PRODUITS_LIVREES.CODE_ARTICLE,	
                T_LIVRAISON.code_vendeur,	
                T_CLIENTS.NOM_CLIENT,	
                T_CLIENTS.RANG,	
                T_ARTICLES.LIBELLE,	
                T_ARTICLES.RANG
            ORDER BY 
                RANG ASC,	
                RANG_T_ ASC
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_date_validation': args[1],
                'Param_code_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])
        
        kwargs['OPTIONAL_ARG_1'] = '''T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}' AND'''
        kwargs['OPTIONAL_ARG_2'] = '''T_LIVRAISON.DATE_VALIDATION = {Param_date_validation} AND'''
        kwargs['OPTIONAL_ARG_3'] = 'T_LIVRAISON.CODE_CLIENT = {Param_code_client} AND'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_livraison'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_date_validation'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_livraison_non_valider(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.MOTIF_NON_VALIDATION AS MOTIF_NON_VALIDATION,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_LIVRAISON.code_secteur AS code_secteur,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE
            FROM 
                T_CLIENTS,	
                T_LIVRAISON
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND
                (
                    T_LIVRAISON.TYPE_MVT = 'L'
                    AND	T_LIVRAISON.code_secteur = {Param_CODE_SECTEUR}
                    AND	T_LIVRAISON.DATE_LIVRAISON = '{Param_DATE_LIVRAISON}'
                    AND	T_LIVRAISON.DATE_VALIDATION = '1900-01-01 00:00:00'
                    AND	T_CLIENTS.CLIENT_EN_COMPTE = 1
                    AND	T_CLIENTS.CAT_CLIENT <> 2
                )
        '''

        try:
            kwargs = {
                'Param_CODE_SECTEUR': args[0],
                'Param_DATE_LIVRAISON': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_DATE_LIVRAISON'] = self.validateDate(kwargs['Param_DATE_LIVRAISON'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_livraisons_unique(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_LIVRAISON.code_secteur AS code_secteur,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_LIVRAISON.code_vendeur AS code_vendeur,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.Type_Livraison AS Type_Livraison,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_LIVRAISON.LIVRAISON_TOURNEE AS LIVRAISON_TOURNEE,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.SUR_COMMANDE AS SUR_COMMANDE
            FROM 
                T_CLIENTS,	
                T_LIVRAISON
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND
                (
                    T_LIVRAISON.code_secteur = {Param_code_secteur}
                    AND	T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_LIVRAISON.TYPE_MVT = 'L'
                    {OPTIONAL_ARG_1}
                    AND	T_LIVRAISON.LIVRAISON_TOURNEE = 0
                    AND	T_LIVRAISON.SUR_COMMANDE = 1
                )
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_livraison': args[1],
                'Param_cat_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in ('Param_code_secteur', 'Param_date_livraison'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_CLIENTS.CAT_CLIENT IN ({Param_cat_client})'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_cat_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_aides_vendeur(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.ACTIF AS ACTIF,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.CODE_INTERNE AS CODE_INTERNE
            FROM 
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.ACTIF = 1
                AND	T_OPERATEUR.FONCTION = 3
        '''
        return query

    
    def Req_ls_alimentation_non_valide(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.COMMENTAIRE AS COMMENTAIRE,	
                T_OPERATIONS_CAISSE.MONTANT AS MONTANT
            FROM 
                T_OPERATIONS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.DATE_VALIDATION = '1900-01-01 00:00:00'
                AND	T_OPERATIONS_CAISSE.TYPE_OPERATION = 'A'
        '''
        return query

    
    def Req_ls_alimentation_valide(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.COMMENTAIRE AS COMMENTAIRE,	
                T_OPERATIONS_CAISSE.MONTANT AS MONTANT
            FROM 
                T_OPERATIONS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.DATE_VALIDATION <> '{Param_date_validation}'
                AND	T_OPERATIONS_CAISSE.TYPE_OPERATION = 'A'
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_alimentations(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.MONTANT AS MONTANT,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION
            FROM 
                T_OPERATIONS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.TYPE_OPERATION = 'A'
                AND	T_OPERATIONS_CAISSE.DATE_VALIDATION = '{Param_date_validation}'
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def REQ_ls_appareil(self, args): #Done
        query = '''
            SELECT 
                T_APPAREIL.CODE_APPAREIL AS CODE_APPAREIL,	
                T_APPAREIL.NUMERO_SERIE AS NUMERO_SERIE,	
                T_APPAREIL.NUMERO_SIM AS NUMERO_SIM,	
                T_APPAREIL.ICCID_SIM AS ICCID_SIM,	
                T_APPAREIL.PIN_SIM AS PIN_SIM,	
                T_APPAREIL.PUK_SIM AS PUK_SIM,	
                T_APPAREIL.FORFAIT_SIM AS FORFAIT_SIM,	
                T_APPAREIL.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_APPAREIL.CODE_AGCE AS CODE_AGCE,	
                T_APPAREIL.OBSERVATION AS OBSERVATION,	
                T_APPAREIL.LOGIN_RDP AS LOGIN_RDP,	
                T_APPAREIL.PWD_RDP AS PWD_RDP,	
                T_APPAREIL.DATE_ADD AS DATE_ADD,	
                T_APPAREIL.DATE_ACCEPTATION AS DATE_ACCEPTATION,	
                T_APPAREIL.DATE_ARRET AS DATE_ARRET,	
                T_APPAREIL.LAST_CONNECTION AS LAST_CONNECTION
            FROM 
                T_APPAREIL
            WHERE 
                {OPTIONAL_ARG_1}
                {OPTIONAL_ARG_2}
            ORDER BY
                NUMERO_SERIE
        '''

        try:
            kwargs = {
                'pAppareil': args[0],
                'pOperateur_Moins1PourNull': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'T_APPAREIL.CODE_APPAREIL = {pAppareil} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['pAppareil'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = 'ISNULL(T_APPAREIL.CODE_OPERATEUR , -1)  = {pOperateur_Moins1PourNull}'
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['pOperateur_Moins1PourNull'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_articles(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.GP_ARTICLE AS GP_ARTICLE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                T_ARTICLES.CODE_AROME AS CODE_AROME,	
                T_ARTICLES.TYPE_CAISSE AS TYPE_CAISSE,	
                T_ARTICLES.TYPE_PALETTE AS TYPE_PALETTE,	
                T_ARTICLES.QTE_PACK AS QTE_PACK,	
                T_ARTICLES.QTE_PALETTE AS QTE_PALETTE,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.AFF_REPARTITION AS AFF_REPARTITION,	
                T_ARTICLES.AFF_COMMANDE AS AFF_COMMANDE,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.ABREVIATION AS ABREVIATION,	
                T_FAMILLE.CODE_FAMILLE AS CODE_FAMILLE,	
                T_FAMILLE.CODE_GAMME AS CODE_GAMME,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                T_PRODUITS.RANG AS RANG_PRODUIT,	
                T_PRIX.PRIX AS PRIX_VENTE,	
                T_ARTICLES.CONDITIONNEMENT2 AS CONDITIONNEMENT2,	
                T_ARTICLES.QTE_PALETTE2 AS QTE_PALETTE2,	
                T_ARTICLES.CODE_BARRE AS CODE_BARRE,	
                T_ARTICLES.TX_COUVERTURE AS TX_COUVERTURE,	
                T_FAMILLE.CONTRAT AS CONTRAT,	
                T_ARTICLES.COMMANDE_MIN AS COMMANDE_MIN
            FROM 
                T_ARTICLES,	
                T_PRIX,	
                T_PRODUITS,	
                T_FAMILLE
            WHERE 
                T_FAMILLE.CODE_FAMILLE = T_PRODUITS.CODE_FAMILLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND
                (
                    T_ARTICLES.ACTIF = 1
                    {OPTIONAL_ARG_1}
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_dt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])
        kwargs['OPTIONAL_ARG_1'] = '''AND T_PRIX.Date_Debut <= '{Param_dt}'
                    AND	T_PRIX.Date_Fin >= '{Param_dt}' '''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_dt'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def req_ls_articles_dispo(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.DISPO AS DISPO
            FROM 
                T_ARTICLES
            WHERE 
                T_ARTICLES.DISPO = 0
        '''
        return query

    
    def Req_ls_articles_export(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.GP_ARTICLE AS GP_ARTICLE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                T_ARTICLES.CODE_AROME AS CODE_AROME,	
                T_ARTICLES.TYPE_CAISSE AS TYPE_CAISSE,	
                T_ARTICLES.TYPE_PALETTE AS TYPE_PALETTE,	
                T_ARTICLES.QTE_PACK AS QTE_PACK,	
                T_ARTICLES.QTE_PALETTE AS QTE_PALETTE,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_ARTICLES.RANG AS RANG,	
                T_PRIX.PRIX AS PRIX_VENTE,	
                T_PRIX.CODE_AGCE AS CODE_AGCE,	
                T_ARTICLES.AFF_REPARTITION AS AFF_REPARTITION,	
                T_ARTICLES.AFF_COMMANDE AS AFF_COMMANDE,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.ABREVIATION AS ABREVIATION,	
                T_FAMILLE.CODE_FAMILLE AS CODE_FAMILLE,	
                T_FAMILLE.CODE_GAMME AS CODE_GAMME,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                T_GAMME.CODE_GAMME AS CODE_GAMME_T_
            FROM 
                T_GAMME,	
                T_FAMILLE,	
                T_PRODUITS,	
                T_ARTICLES,	
                T_PRIX
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_FAMILLE.CODE_FAMILLE = T_PRODUITS.CODE_FAMILLE
                AND		T_GAMME.CODE_GAMME = T_FAMILLE.CODE_GAMME
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_ARTICLES.ACTIF = 1
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'param_dt': args[0],
                'Param_code_agce': args[1],
                'Param_aff_commande': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['param_dt'] = self.validateDate(kwargs['param_dt'])
        
        kwargs['OPTIONAL_ARG_1'] = '''T_PRIX.Date_Debut <= '{param_dt}' AND	T_PRIX.Date_Fin >= '{param_dt}' AND'''
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_PRIX.CODE_AGCE = {Param_code_agce}'
        kwargs['OPTIONAL_ARG_3'] = 'AND	T_ARTICLES.AFF_COMMANDE = {Param_aff_commande}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['param_dt'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_agce'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['Param_aff_commande'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']

        return query.format(**kwargs).format(**kwargs)

    
    def req_ls_articles_livrees_newrest(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_CLIENTS.GROUP_CLIENT AS GROUP_CLIENT
            FROM 
                T_CLIENTS,	
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND		T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_CLIENTS.GROUP_CLIENT = {Param_gp_client}
                    AND	T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                )
        '''

        try:
            kwargs = {
                'Param_gp_client': args[0],
                'Param_date_livraison': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_articles_stat(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.GP_ARTICLE AS GP_ARTICLE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                T_ARTICLES.CODE_AROME AS CODE_AROME,	
                T_ARTICLES.TYPE_CAISSE AS TYPE_CAISSE,	
                T_ARTICLES.TYPE_PALETTE AS TYPE_PALETTE,	
                T_ARTICLES.QTE_PACK AS QTE_PACK,	
                T_ARTICLES.QTE_PALETTE AS QTE_PALETTE,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.AFF_REPARTITION AS AFF_REPARTITION,	
                T_ARTICLES.AFF_COMMANDE AS AFF_COMMANDE,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.ABREVIATION AS ABREVIATION,	
                T_FAMILLE.CODE_FAMILLE AS CODE_FAMILLE,	
                T_FAMILLE.CODE_GAMME AS CODE_GAMME,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                T_PRODUITS.RANG AS RANG_PRODUIT,	
                T_PRIX.PRIX AS PRIX_VENTE,	
                T_ARTICLES.CONDITIONNEMENT2 AS CONDITIONNEMENT2,	
                T_ARTICLES.QTE_PALETTE2 AS QTE_PALETTE2,	
                T_ARTICLES.CODE_BARRE AS CODE_BARRE,	
                T_ARTICLES.TX_COUVERTURE AS TX_COUVERTURE
            FROM 
                T_ARTICLES,	
                T_PRIX,	
                T_PRODUITS,	
                T_FAMILLE
            WHERE 
                T_FAMILLE.CODE_FAMILLE = T_PRODUITS.CODE_FAMILLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND
                (
                    T_ARTICLES.ACTIF_GLOBALE = 1
                    AND	T_PRIX.Date_Debut <= '{Param_dt}'
                    AND	T_PRIX.Date_Fin >= '{Param_dt}'
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_dt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])

        if kwargs['Param_dt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_articles_stock(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_ARTICLES_MAGASINS.CATEGORIE AS CATEGORIE,	
                SUM(T_ARTICLES_MAGASINS.QTE_STOCK) AS la_somme_QTE_STOCK
            FROM 
                T_ARTICLES_MAGASINS,	
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_ARTICLES_MAGASINS.CODE_ARTICLE
                AND
                (
                    T_ARTICLES.ACTIF = 1
                    AND	T_ARTICLES_MAGASINS.CATEGORIE = 'PRODUIT'
                )
            GROUP BY 
                T_ARTICLES.CODE_ARTICLE,	
                T_ARTICLES.LIBELLE,	
                T_ARTICLES.ACTIF,	
                T_ARTICLES_MAGASINS.CATEGORIE,	
                T_ARTICLES.RANG
            ORDER BY 
                RANG ASC
        '''
        return query

    
    def Req_ls_articles_tout(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.GP_ARTICLE AS GP_ARTICLE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                T_ARTICLES.CODE_AROME AS CODE_AROME,	
                T_ARTICLES.TYPE_CAISSE AS TYPE_CAISSE,	
                T_ARTICLES.TYPE_PALETTE AS TYPE_PALETTE,	
                T_ARTICLES.QTE_PACK AS QTE_PACK,	
                T_ARTICLES.QTE_PALETTE AS QTE_PALETTE,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_ARTICLES.RANG AS RANG,	
                T_PRIX.PRIX AS PRIX_VENTE,	
                T_ARTICLES.AFF_REPARTITION AS AFF_REPARTITION,	
                T_ARTICLES.AFF_COMMANDE AS AFF_COMMANDE,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.ABREVIATION AS ABREVIATION,	
                T_FAMILLE.CODE_FAMILLE AS CODE_FAMILLE,	
                T_FAMILLE.CODE_GAMME AS CODE_GAMME,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                T_ARTICLES.ACTIF_GLOBALE AS ACTIF_GLOBALE,	
                T_PRIX.Date_Debut AS Date_Debut,	
                T_PRIX.Date_Fin AS Date_Fin
            FROM 
                T_ARTICLES,	
                T_PRIX,	
                T_PRODUITS,	
                T_FAMILLE
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_FAMILLE.CODE_FAMILLE = T_PRODUITS.CODE_FAMILLE
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_ARTICLES.ACTIF_GLOBALE = 1
                    {OPTIONAL_ARG_2}
                    AND	T_PRIX.Date_Fin >= '{Param_dt}'
                    AND	T_PRIX.Date_Debut <= '{Param_dt}'
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_code_agce': args[0],
                'Param_cat': args[1],
                'Param_dt': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])

        if kwargs['Param_dt'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_PRIX.CODE_AGCE = {Param_code_agce} AND'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_ARTICLES.AFF_COMMANDE = {Param_cat}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_agce'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_cat'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_autorisation_caisserie(self, args): #Done
        query = '''
            SELECT 
                T_AUTORISATION_SOLDE_CAISSERIE.ID_JUSTIFICATION AS ID_JUSTIFICATION,	
                T_AUTORISATION_SOLDE_CAISSERIE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_AUTORISATION_SOLDE_CAISSERIE.DATE_HEURE AS DATE_HEURE,	
                T_AUTORISATION_SOLDE_CAISSERIE.DATE_ECHU AS DATE_ECHU,	
                T_AUTORISATION_SOLDE_CAISSERIE.PAR_ORDRE AS PAR_ORDRE,	
                T_AUTORISATION_SOLDE_CAISSERIE.CS_STD AS CS_STD,	
                T_AUTORISATION_SOLDE_CAISSERIE.CS_PR AS CS_PR,	
                T_AUTORISATION_SOLDE_CAISSERIE.CS_AG AS CS_AG,	
                T_AUTORISATION_SOLDE_CAISSERIE.CS_BLC AS CS_BLC,	
                T_AUTORISATION_SOLDE_CAISSERIE.PAL_AG AS PAL_AG,	
                T_AUTORISATION_SOLDE_CAISSERIE.PAL_UHT AS PAL_UHT,	
                T_AUTORISATION_SOLDE_CAISSERIE.PAL_EURO AS PAL_EURO,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.Matricule AS Matricule,	
                SAISIE_PAR.NOM_OPERATEUR AS NOM_OPERATEUR_SA,	
                ORDRE.NOM_OPERATEUR AS NOM_OPERATEUR_OR
            FROM 
                T_AUTORISATION_SOLDE_CAISSERIE,	
                T_OPERATEUR SAISIE_PAR,	
                T_OPERATEUR,	
                T_OPERATEUR ORDRE
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_AUTORISATION_SOLDE_CAISSERIE.CODE_OPERATEUR
                AND		SAISIE_PAR.CODE_OPERATEUR = T_AUTORISATION_SOLDE_CAISSERIE.SAISIE_PAR
                AND		ORDRE.CODE_OPERATEUR = T_AUTORISATION_SOLDE_CAISSERIE.PAR_ORDRE
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_AUTORISATION_SOLDE_CAISSERIE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in ('Param_dt1', 'Param_dt2'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_AUTORISATION_SOLDE_CAISSERIE.CODE_OPERATEUR = {Param_code_operateur} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_operateur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_autorisation_journee(self, args): #Done
        query = '''
            SELECT 
                T_AUTORISATIONS_SOLDE.ID_JUSTIFICATION AS ID_JUSTIFICATION,	
                T_AUTORISATIONS_SOLDE.DATE_OPERATION AS DATE_OPERATION
            FROM 
                T_AUTORISATIONS_SOLDE
            WHERE 
                T_AUTORISATIONS_SOLDE.DATE_OPERATION = '{Param_date_operation}'
        '''
        
        try:
            kwargs = {
                'Param_date_operation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_operation'] = self.validateDate(kwargs['Param_date_operation'])

        if kwargs['Param_date_operation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_autorisations(self, args): #Done
        query = '''
            SELECT 
                T_AUTORISATIONS_SOLDE.ID_JUSTIFICATION AS ID_JUSTIFICATION,	
                T_AUTORISATIONS_SOLDE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_AUTORISATIONS_SOLDE.DATE_OPERATION AS DATE_OPERATION,	
                T_AUTORISATIONS_SOLDE.CREER_PAR AS CREER_PAR,	
                T_AUTORISATIONS_SOLDE.ORDRE_PAR AS ORDRE_PAR,	
                T_AUTORISATIONS_SOLDE.MONTANT AS MONTANT,	
                T_AUTORISATIONS_SOLDE.DATE_ECHU AS DATE_ECHU,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.Matricule AS Matricule,	
                ORDRE.NOM_OPERATEUR AS NOM_OPERATEUR_OR,	
                OP_SAISIE.NOM_OPERATEUR AS NOM_OPERATEUR_OP
            FROM 
                T_OPERATEUR,	
                T_AUTORISATIONS_SOLDE,	
                T_OPERATEUR ORDRE,	
                T_OPERATEUR OP_SAISIE
            WHERE 
                OP_SAISIE.CODE_OPERATEUR = T_AUTORISATIONS_SOLDE.CREER_PAR
                AND		ORDRE.CODE_OPERATEUR = T_AUTORISATIONS_SOLDE.ORDRE_PAR
                AND		T_OPERATEUR.CODE_OPERATEUR = T_AUTORISATIONS_SOLDE.CODE_OPERATEUR
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_AUTORISATIONS_SOLDE.DATE_OPERATION BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in ('Param_dt1', 'Param_dt2'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_AUTORISATIONS_SOLDE.CODE_OPERATEUR = {Param_code_operateur} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_operateur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_avoirs_secteurs(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.COMPTE_ECART AS COMPTE_ECART,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                SUM(T_PRODUITS_CHARGEE.QTE_ECART) AS la_somme_QTE_ECART,	
                SUM(T_PRODUITS_CHARGEE.MONTANT_ECART) AS la_somme_MONTANT_ECART,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT
            FROM 
                T_PRODUITS,	
                T_ARTICLES,	
                T_PRODUITS_CHARGEE,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_PRODUITS_CHARGEE.code_secteur
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_CHARGEE.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND
                (
                    T_PRODUITS_CHARGEE.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_PRODUITS_CHARGEE.COMPTE_ECART = {Param_combo_controleur}
                )
            GROUP BY 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.COMPTE_ECART,	
                T_SECTEUR.NOM_SECTEUR,	
                T_PRODUITS.NOM_PRODUIT
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_combo_controleur': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_avoirs_secteurs_caisserie(self, args): #Done
        query = '''
            SELECT 
                T_COND_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_COND_CHARGEE.COMPTE_ECART AS COMPTE_ECART,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                SUM(T_COND_CHARGEE.ECART) AS la_somme_ECART
            FROM 
                T_CHARGEMENT,	
                T_COND_CHARGEE,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_COND_CHARGEE.CODE_CHARGEMENT
                AND
                (
                    T_COND_CHARGEE.COMPTE_ECART = {Param_compte_ecart}
                    AND	T_COND_CHARGEE.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_SECTEUR.NOM_SECTEUR,	
                T_COND_CHARGEE.CODE_COND,	
                T_COND_CHARGEE.COMPTE_ECART,	
                T_COND_CHARGEE.DATE_CHARGEMENT
        '''

        try:
            kwargs = {
                'Param_compte_ecart': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_bl_client(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_LIVRAISON.STATUT AS STATUT,	
                T_LIVRAISON.NUM_COMMANDE AS NUM_COMMANDE,	
                T_LIVRAISON.SUR_COMMANDE AS SUR_COMMANDE,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT
            FROM 
                T_CLIENTS,	
                T_LIVRAISON
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND
                (
                    T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_LIVRAISON.CODE_CLIENT = {Param_code_client}
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_client': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_borderau_valeurs(self, args): #Done
        query = '''
            SELECT 
                T_BORDEREAU_VALEURS.ID_BORDEREAU AS ID_BORDEREAU,	
                T_BORDEREAU_VALEURS.VALID AS VALID,	
                T_BORDEREAU_VALEURS.DATE_HEURE_SAISIE AS DATE_HEURE_SAISIE,	
                T_BORDEREAU_VALEURS.ACTIF AS ACTIF,	
                T_BORDEREAU_VALEURS.NUM_BORDEAU_BANQUE AS NUM_BORDEAU_BANQUE,	
                T_BORDEREAU_VALEURS.DATE_SITUATION AS DATE_SITUATION,	
                T_BANQUES.LIBELLE AS LIBELLE
            FROM 
                T_BANQUES,	
                T_BORDEREAU_VALEURS
            WHERE 
                T_BANQUES.NUM_BANQUE = T_BORDEREAU_VALEURS.CODE_BANQUE
                AND
                (
                    T_BORDEREAU_VALEURS.ACTIF = 1
                )
            ORDER BY 
                ID_BORDEREAU DESC
        '''
        return query

    
    def Req_ls_chargement_cac(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.CODE_CHARGEMENT AS CODE_CHARGEMENT
            FROM 
                T_CHARGEMENT
            WHERE 
                T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_chargements_journee(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_CHARGEMENT.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE AS CODE_TOURNEE,	
                T_CHARGEMENT.vehicule AS vehicule,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_CHARGEMENT.code_chauffeur AS code_chauffeur,	
                T_CHARGEMENT.AIDE_VENDEUR1 AS AIDE_VENDEUR1,	
                T_CHARGEMENT.AIDE_VENDEUR2 AS AIDE_VENDEUR2,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_VENDEUR,	
                T_TOURNEES.NOM_TOURNEE AS NOM_TOURNEE,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_CHARGEMENT.VALID AS VALID,	
                T_CHARGEMENT.MONTANT_A_VERSER AS MONTANT_A_VERSER,	
                T_SECTEUR.CAT_SECTEUR AS CAT_SECTEUR,	
                T_SECTEUR.RANG AS RANG,	
                T_SECTEUR.PREVENTE AS PREVENTE
            FROM 
                T_SECTEUR,	
                T_TOURNEES,	
                T_CHARGEMENT,	
                T_OPERATEUR
            WHERE 
                T_CHARGEMENT.CODE_TOURNEE = T_TOURNEES.CODE_TOURNEE
                AND		T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_CHARGEMENT.code_vendeur = T_OPERATEUR.CODE_OPERATEUR
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError
    
        return query.format(**kwargs)

    
    def Req_ls_chauffeurs(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.ACTIF AS ACTIF
            FROM 
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.ACTIF = 1
                AND	T_OPERATEUR.FONCTION IN (2, 5, 6) 
        '''
        return query

    
    def Req_ls_cheques(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.MONTANT AS MONTANT,	
                T_OPERATIONS_CAISSE.NUM_PIECE AS NUM_PIECE,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION
            FROM 
                T_OPERATIONS_CAISSE
            WHERE 
                {OPTIONAL_ARG_1}
                T_OPERATIONS_CAISSE.TYPE_OPERATION = 'T'
                AND	T_OPERATIONS_CAISSE.DATE_VALIDATION = '{Param_date_validation}'
        '''

        try:
            kwargs = {
                'Param_date_operation': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_operation'] = self.validateDate(kwargs['Param_date_operation'])
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = '''T_OPERATIONS_CAISSE.DATE_OPERATION = '{Param_date_operation}' AND'''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_operation'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_cheques_cac(self, args):
        query = '''
            SELECT 
                T_DECOMPTE.NUM_DECOMPTE AS NUM_DECOMPTE,	
                T_DECOMPTE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_DECOMPTE.DATE_DECOMPTE AS DATE_DECOMPTE,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT,	
                T_DECOMPTE.CODE_CAISSE AS CODE_CAISSE,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_DECOMPTE.DATE_HEURE_VERS AS DATE_HEURE_VERS,	
                T_DECOMPTE.CODE_CLIENT AS CODE_CLIENT,	
                T_DECOMPTE.CODE_BANQUE AS CODE_BANQUE,	
                T_DECOMPTE.REFERENCE AS REFERENCE,	
                T_DECOMPTE.REGLEMENT AS REGLEMENT,	
                T_DT_DECOMPTE.GP_CLIENT AS GP_CLIENT,	
                T_GROUP_CLIENTS.NOM_GROUP AS NOM_GROUP,	
                T_DT_DECOMPTE.RIB AS RIB
            FROM 
                T_DT_DECOMPTE,	
                T_GROUP_CLIENTS,	
                T_DECOMPTE
            WHERE 
                T_DT_DECOMPTE.GP_CLIENT = T_GROUP_CLIENTS.ID_GP_CLIENT
                AND		T_DECOMPTE.NUM_DECOMPTE = T_DT_DECOMPTE.NUM_DECOMPTE
                AND
                (
                    T_DECOMPTE.MODE_PAIEMENT = 'R'
                    AND	T_DECOMPTE.DATE_HEURE_VERS BETWEEN '{Param1}' AND '{Param2}'
                    {OPTIONAL_ARG_1}
                )
            ORDER BY 
                NUM_DECOMPTE DESC
        '''
        try:
            kwargs = {
                'Param1': args[0],
                'Param2': args[1],
                'Param_gp_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param1'] = self.validateDate(kwargs['Param1'], 0)
        kwargs['Param2'] = self.validateDate(kwargs['Param2'], 1)

        kwargs['OPTIONAL_ARG_1'] = 'AND T_DT_DECOMPTE.GP_CLIENT = {Param_gp_client}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_gp_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_cheques_non_remis(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.NUM_DECOMPTE AS NUM_DECOMPTE,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_BANQUES.LIBELLE AS LIBELLE,	
                T_DT_DECOMPTE.LE_TIRE AS LE_TIRE,	
                T_DT_DECOMPTE.DATE_CHEQUE AS DATE_CHEQUE,	
                T_DT_DECOMPTE.DATE_ECHEANCE AS DATE_ECHEANCE,	
                T_DECOMPTE.REFERENCE AS REFERENCE
            FROM 
                T_DT_DECOMPTE,	
                T_BANQUES,	
                T_DECOMPTE
            WHERE 
                T_DECOMPTE.NUM_DECOMPTE = T_DT_DECOMPTE.NUM_DECOMPTE
                AND		T_BANQUES.NUM_BANQUE = T_DECOMPTE.CODE_BANQUE
                AND
                (
                    T_DECOMPTE.NUM_DECOMPTE NOT IN 
                    (
                        SELECT 
                            T_DT_BORDEREAU.NUM_DECOMPTE AS NUM_DECOMPTE
                        FROM 
                            T_BORDEREAU_VALEURS,	
                            T_DT_BORDEREAU
                        WHERE 
                            T_BORDEREAU_VALEURS.ID_BORDEREAU = T_DT_BORDEREAU.ID_BORDEREAU
                        AND
                            (
                                T_BORDEREAU_VALEURS.ACTIF = 1
                            )
                    ) 
                )
        '''
        return query

    
    def Req_ls_client_servi_date(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_FACTURE.VALID AS VALID
            FROM 
                T_FACTURE,	
                T_CLIENTS,	
                T_SOUS_SECTEUR
            WHERE 
                T_FACTURE.CODE_CLIENT = T_CLIENTS.CODE_CLIENT
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_DATE_HEURE1}' AND '{Param_DATE_HEURE2}'
                    AND	T_SOUS_SECTEUR.code_secteur = {Param_CODE_SECTEUR}
                )
        '''

        try:
            kwargs = {
                'Param_DATE_HEURE1': args[0],
                'Param_DATE_HEURE2': args[1],
                'Param_CODE_SECTEUR': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_DATE_HEURE1'] = self.validateDate(kwargs['Param_DATE_HEURE1'])
        kwargs['Param_DATE_HEURE2'] = self.validateDate(kwargs['Param_DATE_HEURE2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_clients(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.RANG AS RANG,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_CAT_CLIENTS.NOM_CATEGORIE AS NOM_CATEGORIE,	
                T_CLIENTS.CLASSE AS CLASSE,	
                T_CLASSE_CLIENTS.NOM_CLASSE AS NOM_CLASSE,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.DES_CLIENT AS DES_CLIENT,	
                T_CLIENTS.ADRESSE AS ADRESSE,	
                T_CLIENTS.TELEPHONE AS TELEPHONE,	
                T_CLIENTS.SOUS_SECTEUR AS SOUS_SECTEUR,	
                T_CLIENTS.TOURNEE AS TOURNEE,	
                T_CLIENTS.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_CLIENTS.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_CLIENTS.SOLDE_C_STD AS SOLDE_C_STD,	
                T_CLIENTS.SOLDE_P_AG AS SOLDE_P_AG,	
                T_CLIENTS.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_CLIENTS.SOLDE_C_AG AS SOLDE_C_AG,	
                T_CLIENTS.SOLDE_C_PR AS SOLDE_C_PR,	
                T_CLIENTS.TYPE_BL AS TYPE_BL,	
                T_CLIENTS.GROUP_CLIENT AS GROUP_CLIENT,	
                T_CLIENTS.AUT_CHEQUE AS AUT_CHEQUE,	
                T_CLIENTS.TYPE_PRESENTOIRE AS TYPE_PRESENTOIRE,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_CLIENTS.REMISE_LAIT AS REMISE_LAIT
            FROM 
                T_CAT_CLIENTS,	
                T_CLIENTS,	
                T_CLASSE_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLASSE_CLIENTS.CODE_CLASSE = T_CLIENTS.CLASSE
                AND		T_CAT_CLIENTS.CODE_CAT_CLIENT = T_CLIENTS.CAT_CLIENT
                AND
                (
                    T_CLIENTS.CODE_CLIENT <> 0
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                    {OPTIONAL_ARG_4}
                    {OPTIONAL_ARG_5}
                    {OPTIONAL_ARG_6}
                )
        '''

        try:
            kwargs = {
                'Param_param_code_secteur': args[0],
                'Param_cac': args[1],
                'param_not_classe': args[2],
                'Param_auth_cheque': args[3],
                'Param_type_pres': args[4],
                'Param_actif': args[5]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SOUS_SECTEUR.code_secteur = {Param_param_code_secteur}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_CLIENTS.CLIENT_EN_COMPTE = {Param_cac}'
        kwargs['OPTIONAL_ARG_3'] = 'AND	T_CLIENTS.CLASSE NOT IN ({param_not_classe})'
        kwargs['OPTIONAL_ARG_4'] = 'AND	T_CLIENTS.AUT_CHEQUE = {Param_auth_cheque}'
        kwargs['OPTIONAL_ARG_5'] = 'AND	T_CLIENTS.TYPE_PRESENTOIRE = {Param_type_pres}'
        kwargs['OPTIONAL_ARG_6'] = 'AND	T_CLIENTS.ACTIF = {Param_actif}'
        
        keys = ('Param_param_code_secteur', 'Param_cac', 'param_not_classe', 'Param_auth_cheque', 'Param_type_pres', 'Param_actif')
        for arg, key in zip(range(1, 7), keys):
            if kwargs[key] in (None, 'NULL'):
                kwargs['OPTIONAL_ARG_{}'.format(arg)] = ''

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_clients_cac(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE
            FROM 
                T_CLIENTS
            WHERE 
                T_CLIENTS.CLIENT_EN_COMPTE = 1
                AND	T_CLIENTS.ACTIF = 1
        '''
        return query

    
    def Req_ls_clients_cac_dep(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.RANG AS RANG,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_SECTEUR.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.GROUP_CLIENT AS GROUP_CLIENT
            FROM 
                T_SECTEUR,	
                T_SOUS_SECTEUR,	
                T_CLIENTS
            WHERE 
                T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_CLIENTS.ACTIF = 1
                    AND	
                    (
                        T_CLIENTS.CLIENT_EN_COMPTE = 1
                        OR	T_CLIENTS.CAT_CLIENT IN (1, 2, 15) 
                    )
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_GP_CLIENT': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SECTEUR.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_CLIENTS.GROUP_CLIENT = {Param_GP_CLIENT}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_GP_CLIENT'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_clients_cac_remise(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_CLIENTS.CLASSE AS CLASSE,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE
            FROM 
                T_CLIENTS,	
                T_LIVRAISON,	
                T_SOUS_SECTEUR,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND
                (
                    T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.DATE_LIVRAISON BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_CLIENTS.CLIENT_EN_COMPTE = 1
                    AND	T_CLIENTS.CLASSE IN (2, 6, 7, 8, 9, 10) 
                )
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_ls_clients_classe_secteur(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.CLASSE AS CLASSE,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur
            FROM 
                T_SOUS_SECTEUR,	
                T_CLIENTS
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
                    AND	T_CLIENTS.CLASSE NOT IN (1, 3) 
                )
        '''
        
        try:
            kwargs = {
                'Param_code_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_clients_con_dec(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT
            FROM 
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT <> 0
                AND	T_CLIENTS.SOLDE_C_PR = 1
            ORDER BY 
                NOM_CLIENT ASC
        '''
        return query

    
    def Req_ls_clients_conseigne(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.RANG AS RANG,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_CAT_CLIENTS.NOM_CATEGORIE AS NOM_CATEGORIE,	
                T_CLIENTS.CLASSE AS CLASSE,	
                T_CLASSE_CLIENTS.NOM_CLASSE AS NOM_CLASSE,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.DES_CLIENT AS DES_CLIENT,	
                T_CLIENTS.ADRESSE AS ADRESSE,	
                T_CLIENTS.TELEPHONE AS TELEPHONE,	
                T_CLIENTS.SOUS_SECTEUR AS SOUS_SECTEUR,	
                T_CLIENTS.TOURNEE AS TOURNEE,	
                T_CLIENTS.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_CLIENTS.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_CLIENTS.SOLDE_C_STD AS SOLDE_C_STD,	
                T_CLIENTS.SOLDE_P_AG AS SOLDE_P_AG,	
                T_CLIENTS.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_CLIENTS.SOLDE_C_AG AS SOLDE_C_AG,	
                T_CLIENTS.SOLDE_C_PR AS SOLDE_C_PR,	
                T_CLIENTS.TYPE_BL AS TYPE_BL,	
                T_CLIENTS.GROUP_CLIENT AS GROUP_CLIENT,	
                T_CLIENTS.AUT_CHEQUE AS AUT_CHEQUE,	
                T_CLIENTS.TYPE_PRESENTOIRE AS TYPE_PRESENTOIRE,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR
            FROM 
                T_CAT_CLIENTS,	
                T_CLIENTS,	
                T_CLASSE_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLASSE_CLIENTS.CODE_CLASSE = T_CLIENTS.CLASSE
                AND		T_CAT_CLIENTS.CODE_CAT_CLIENT = T_CLIENTS.CAT_CLIENT
                AND
                (
                    T_CLIENTS.CODE_CLIENT <> 0
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                    {OPTIONAL_ARG_4}
                    {OPTIONAL_ARG_5}
                    {OPTIONAL_ARG_6}
                    AND	T_CLIENTS.SOLDE_C_PR = 1
                )
        '''

        try:
            kwargs = {
                'Param_param_code_secteur': args[0],
                'Param_cac': args[1],
                'param_not_classe': args[2],
                'Param_auth_cheque': args[3],
                'Param_type_pres': args[4],
                'Param_actif': args[5]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SOUS_SECTEUR.code_secteur = {Param_param_code_secteur}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_CLIENTS.CLIENT_EN_COMPTE = {Param_cac}'
        kwargs['OPTIONAL_ARG_3'] = 'AND	T_CLIENTS.CLASSE NOT IN ({param_not_classe})'
        kwargs['OPTIONAL_ARG_4'] = 'AND	T_CLIENTS.AUT_CHEQUE = {Param_auth_cheque}'
        kwargs['OPTIONAL_ARG_5'] = 'AND	T_CLIENTS.TYPE_PRESENTOIRE = {Param_type_pres}'
        kwargs['OPTIONAL_ARG_6'] = 'AND	T_CLIENTS.ACTIF = {Param_actif}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_cac'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['param_not_classe'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']
        kwargs['OPTIONAL_ARG_4'] = '' if kwargs['Param_auth_cheque'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_4']
        kwargs['OPTIONAL_ARG_5'] = '' if kwargs['Param_type_pres'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_5']
        kwargs['OPTIONAL_ARG_6'] = '' if kwargs['Param_actif'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_6']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_clients_itinéraire(self, args): #Done
        query = '''
            SELECT 
                T_ITINERAIRES.CODE_TOURNEE AS CODE_TOURNEE,	
                T_ITINERAIRES.CODE_CLIENT AS CODE_CLIENT,	
                T_ITINERAIRES.RANG AS RANG,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.ADRESSE AS ADRESSE,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_ITINERAIRES.RANG_PREVENTE AS RANG_PREVENTE
            FROM 
                T_CLIENTS,	
                T_ITINERAIRES,	
                T_SOUS_SECTEUR
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLIENTS.CODE_CLIENT = T_ITINERAIRES.CODE_CLIENT
                AND
                (
                    T_CLIENTS.ACTIF = 1
                    AND	T_ITINERAIRES.CODE_TOURNEE = {Param_code_tournee}
                )
            ORDER BY 
                RANG ASC
        '''
        
        try:
            kwargs = {
                'Param_code_tournee': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_tournee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_clients_remise_lait(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_CLIENTS.REMISE_LAIT AS REMISE_LAIT
            FROM 
                T_CLIENTS
            WHERE 
                T_CLIENTS.ACTIF = 1
                AND	T_CLIENTS.REMISE_LAIT = 1
        '''
        return query

    
    def Req_ls_clients_sans_facture(self, args): #Done
        query = '''
            SELECT 
                T_ITINERAIRES.CODE_TOURNEE AS CODE_TOURNEE,	
                T_ITINERAIRES.CODE_CLIENT AS CODE_CLIENT,	
                T_ITINERAIRES.RANG AS RANG,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.ADRESSE AS ADRESSE,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_ITINERAIRES.RANG_PREVENTE AS RANG_PREVENTE
            FROM 
                T_CLIENTS,	
                T_ITINERAIRES,	
                T_SOUS_SECTEUR
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLIENTS.CODE_CLIENT = T_ITINERAIRES.CODE_CLIENT
                AND
                (
                    T_CLIENTS.ACTIF = 1
                    AND	T_ITINERAIRES.CODE_TOURNEE = {Param_code_tournee}
                    AND	T_ITINERAIRES.CODE_CLIENT NOT IN ({Param_clts}) 
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_code_tournee': args[0],
                'Param_clts': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_clients_secteur(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.RANG AS RANG,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_SECTEUR.code_secteur AS code_secteur,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.CLASSE AS CLASSE,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_CLIENTS.SOLDE_C_STD AS SOLDE_C_STD,	
                T_CLIENTS.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_CLIENTS.TYPE_PRESENTOIRE AS TYPE_PRESENTOIRE
            FROM 
                T_SECTEUR,	
                T_SOUS_SECTEUR,	
                T_CLIENTS
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND
                (
                    T_CLIENTS.ACTIF = 1
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                    {OPTIONAL_ARG_4}
                )
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_cac': args[1],
                'Param_classe': args[2],
                'Param_categorie': args[3]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SECTEUR.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_CLIENTS.CLIENT_EN_COMPTE = {Param_cac}'
        kwargs['OPTIONAL_ARG_3'] = 'AND	T_CLIENTS.CLASSE IN ({Param_classe})'
        kwargs['OPTIONAL_ARG_4'] = 'AND	T_CLIENTS.CAT_CLIENT = {Param_categorie}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_cac'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['Param_classe'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']
        kwargs['OPTIONAL_ARG_4'] = '' if kwargs['Param_categorie'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_4']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_code_secteur_commandes(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_COMMANDES.code_secteur AS code_secteur
            FROM 
                T_COMMANDES
            WHERE 
                T_COMMANDES.TYPE_COMMANDE = 'S'
                AND	T_COMMANDES.DATE_LIVRAISON = {Param_date_livraison}
        '''
        
        try:
            kwargs = {
                'Param_date_livraison': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_commande(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.ID_COMMANDE AS ID_COMMANDE,	
                T_COMMANDES.TYPE_COMMANDE AS TYPE_COMMANDE,	
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COMMANDES.code_secteur AS code_secteur,	
                T_COMMANDES.CODE_CLIENT AS CODE_CLIENT,	
                T_COMMANDES.CODE_AGENCE AS CODE_AGENCE,	
                T_COMMANDES.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_COMMANDES.DATE_HEURE_SAISIE AS DATE_HEURE_SAISIE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_COMMANDES.NUM_COMMANDE AS NUM_COMMANDE,	
                T_COMMANDES.DATE_COMMANDE AS DATE_COMMANDE,	
                T_COMMANDES.OS AS OS
            FROM 
                T_COMMANDES,	
                T_OPERATEUR,	
                T_CLIENTS,	
                T_SECTEUR
            WHERE 
                T_COMMANDES.code_secteur = T_SECTEUR.code_secteur
                AND		T_CLIENTS.CODE_CLIENT = T_COMMANDES.CODE_CLIENT
                AND		T_COMMANDES.CODE_OPERATEUR = T_OPERATEUR.CODE_OPERATEUR
                AND
                (
                    T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_COMMANDES.CODE_AGENCE = {Param_code_agence}
                    AND	T_COMMANDES.TYPE_COMMANDE IN ({Param_type_commande}) 
                )
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_agence': args[1],
                'Param_type_commande': args[2]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_commande_client(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.ID_COMMANDE AS ID_COMMANDE,	
                T_COMMANDES.TYPE_COMMANDE AS TYPE_COMMANDE,	
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COMMANDES.code_secteur AS code_secteur,	
                T_COMMANDES.CODE_CLIENT AS CODE_CLIENT,	
                T_COMMANDES.CODE_AGENCE AS CODE_AGENCE,	
                T_COMMANDES.NUM_COMMANDE AS NUM_COMMANDE,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_COMMANDES.OS AS OS
            FROM 
                T_CLIENTS,	
                T_COMMANDES
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_COMMANDES.CODE_CLIENT
                AND
                (
                    T_COMMANDES.TYPE_COMMANDE = 'C'
                    AND	T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}'
                    {OPTIONAL_ARG_1}
                )
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_COMMANDES.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_commande_usine(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.ID_COMMANDE AS ID_COMMANDE,	
                T_COMMANDES.TYPE_COMMANDE AS TYPE_COMMANDE,	
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COMMANDES.code_secteur AS code_secteur,	
                T_COMMANDES.CODE_AGENCE AS CODE_AGENCE,	
                T_COMMANDES.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_COMMANDES.DATE_HEURE_SAISIE AS DATE_HEURE_SAISIE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_COMMANDES.NUM_COMMANDE AS NUM_COMMANDE
            FROM 
                T_COMMANDES,	
                T_OPERATEUR
            WHERE 
                T_COMMANDES.CODE_OPERATEUR = T_OPERATEUR.CODE_OPERATEUR
                AND
                (
                    T_COMMANDES.CODE_AGENCE = {Param_code_agence}
                    AND	T_COMMANDES.TYPE_COMMANDE IN ({Param_type_commande}) 
                )
            ORDER BY 
                DATE_LIVRAISON DESC
        '''

        try:
            kwargs = {
                'Param_code_agence': args[0],
                'Param_type_commande': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def req_ls_commandes_prevente(self, args):
        query = '''
            SELECT 
                T_COMMANDE_CLIENT.ID_COMMANDE AS ID_COMMANDE,	
                T_COMMANDE_CLIENT.DATE_COMMANDE AS DATE_COMMANDE,	
                T_COMMANDE_CLIENT.CODE_CLIENT AS CODE_CLIENT,	
                T_COMMANDE_CLIENT.DATE_HEURE_SAISIE AS DATE_HEURE_SAISIE,	
                T_COMMANDE_CLIENT.CODE_PREVENDEUR AS CODE_PREVENDEUR,	
                T_COMMANDE_CLIENT.STATUT AS STATUT,	
                T_COMMANDE_CLIENT.ETAT AS ETAT,	
                T_COMMANDE_CLIENT.NUM_FACTURE AS NUM_FACTURE,	
                T_COMMANDE_CLIENT.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_ZONE.NOM_ZONE AS NOM_ZONE,	
                T_ZONE.CODE_SUPERVISEUR AS CODE_SUPERVISEUR,	
                T_ZONE.RESP_VENTE AS RESP_VENTE,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_COMMANDE_CLIENT.DATE_HEURE_LIVRAISON AS DATE_HEURE_LIVRAISON,	
                T_COMMANDE_CLIENT.CODE_LIVREUR AS CODE_LIVREUR
            FROM 
                T_ZONE,	
                T_BLOC,	
                T_SECTEUR,	
                T_CLIENTS,	
                T_COMMANDE_CLIENT
            WHERE 
                T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND		T_SECTEUR.code_secteur = T_COMMANDE_CLIENT.code_secteur
                AND		T_CLIENTS.CODE_CLIENT = T_COMMANDE_CLIENT.CODE_CLIENT
                AND
                (
                    T_COMMANDE_CLIENT.DATE_COMMANDE BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_COMMANDE_CLIENT.code_secteur = {Param_code_secteur}
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_ZONE.RESP_VENTE = {Param_resp_vente}
                    AND	T_COMMANDE_CLIENT.CODE_CLIENT = {Param_code_client}
                )
        '''
        return query.format(**kwargs)

    
    def Req_ls_comptes(self, args): #Done
        query = ''' 
            SELECT 
                T_COMPTES.CODE_COMPTE AS CODE_COMPTE,	
                T_COMPTES.NUM_COMPTE AS NUM_COMPTE,	
                T_BANQUES.LIBELLE AS LIBELLE
            FROM 
                T_BANQUES,	
                T_COMPTES
            WHERE 
                T_BANQUES.NUM_BANQUE = T_COMPTES.BANQUE
                AND
                (
                    T_COMPTES.CODE_COMPTE <> 0
                )
        '''
        return query

    
    def Req_ls_cond(self, args): #Done
        query = '''
            SELECT 
                T_MAGASIN_COND.CODE_CP AS CODE_CP,	
                T_CAISSES_PALETTES.NOM_TYPE AS NOM_TYPE,	
                T_MAGASIN_COND.QTE_STOCK AS QTE_STOCK,	
                T_CAISSES_PALETTES.CATEGORIE AS CATEGORIE,	
                T_CAISSES_PALETTES.PRIX_VENTE AS PRIX_VENTE,	
                T_MAGASIN_COND.CODE_MAGASIN AS CODE_MAGASIN
            FROM 
                T_CAISSES_PALETTES,	
                T_MAGASIN_COND
            WHERE 
                T_CAISSES_PALETTES.CODE_TYPE = T_MAGASIN_COND.CODE_CP
                AND
                (
                    T_MAGASIN_COND.CODE_CP <> 99
                    AND	T_MAGASIN_COND.CODE_MAGASIN = {Param_code_magasin}
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
        '''

        try:
            kwargs = {
                'Param_code_magasin': args[0],
                'Param_code_cp': args[1],
                'Param_type_cond': args[2]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_code_magasin'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_MAGASIN_COND.CODE_CP = {Param_code_cp}'
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_CAISSES_PALETTES.CATEGORIE = {Param_type_cond}'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_cp'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_type_cond'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_cond_chargees(self, args): #Done
        query = '''
            SELECT 
                T_COND_CHARGEE.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                T_COND_CHARGEE.QTE_CHARGEE AS QTE_CHARGEE,	
                T_COND_CHARGEE.QTE_CHARGEE_VAL AS QTE_CHARGEE_VAL,	
                T_COND_CHARGEE.QTE_POINTE AS QTE_POINTE,	
                T_COND_CHARGEE.QTE_CHAR_SUPP AS QTE_CHAR_SUPP,	
                T_COND_CHARGEE.QTE_RETOUR AS QTE_RETOUR,	
                T_COND_CHARGEE.ECART AS ECART,	
                T_COND_CHARGEE.VALEUR_ECART AS VALEUR_ECART,	
                T_COND_CHARGEE.COMPTE_ECART AS COMPTE_ECART,	
                T_COND_CHARGEE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_COND_CHARGEE.CREDIT AS CREDIT,	
                T_COND_CHARGEE.VALEUR_CREDIT AS VALEUR_CREDIT
            FROM 
                T_COND_CHARGEE
            WHERE 
                T_COND_CHARGEE.CODE_CHARGEMENT = {Param_code_chargement}
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_code_chargement': args[0],
                'Param_code_cond': args[1]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_code_chargement'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_COND_CHARGEE.CODE_COND = {Param_code_cond}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_cond'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_cond_livraison(self, args): #Done
        query = '''
            SELECT 
                T_COND_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                T_COND_LIVRAISON.QTE_CHARGEE AS QTE_CHARGEE,	
                T_COND_LIVRAISON.VALEUR_CHARGEE AS VALEUR_CHARGEE,	
                T_COND_LIVRAISON.PRIX AS PRIX,	
                T_CAISSES_PALETTES.NOM_TYPE AS NOM_TYPE,	
                T_COND_LIVRAISON.CODE_MAGASIN AS CODE_MAGASIN,	
                T_COND_LIVRAISON.QTE_IMPORTE AS QTE_IMPORTE
            FROM 
                T_CAISSES_PALETTES,	
                T_COND_LIVRAISON
            WHERE 
                T_CAISSES_PALETTES.CODE_TYPE = T_COND_LIVRAISON.CODE_CP
                AND
                (
                    T_COND_LIVRAISON.NUM_LIVRAISON = {Param_num_livraison}
                )
        '''
        
        try:
            kwargs = {
                'Param_num_livraison': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_num_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_cond_livrees(self, args): #Done
        query = '''
            SELECT 
                T_COND_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                T_COND_LIVRAISON.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_COND_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_COND_LIVRAISON.TYPE_CLIENT AS TYPE_CLIENT,	
                T_COND_LIVRAISON.QTE_IMPORTE AS QTE_IMPORTE,	
                T_COND_LIVRAISON.QTE_CHARGEE AS QTE_CHARGEE,	
                T_COND_LIVRAISON.PRIX AS PRIX
            FROM 
                T_COND_LIVRAISON
            WHERE 
                T_COND_LIVRAISON.NUM_LIVRAISON = {Param_num_livraison}
                AND	T_COND_LIVRAISON.CODE_CP = {Param_code_cp}
        '''

        try:
            kwargs = {
                'Param_num_livraison': args[0],
                'Param_code_cp': args[1]
            }
        except IndexError as e:
            return e

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_controlleurs(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.CODE_INTERNE AS CODE_INTERNE
            FROM 
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.FONCTION = 7
        '''
        return query

    
    def Req_ls_courrier(self, args): #Done
        query = '''
            SELECT 
                T_COURRIER_AGENCE.ID_ENVOI AS ID_ENVOI,	
                T_COURRIER_AGENCE.CAT AS CAT,	
                T_COURRIER_AGENCE.DATE_HEURE_SAISIE AS DATE_HEURE_SAISIE,	
                T_COURRIER_AGENCE.DATE_HEURE_ENVOI AS DATE_HEURE_ENVOI,	
                T_COURRIER_AGENCE.SAISIE_PAR AS SAISIE_PAR,	
                T_COURRIER_AGENCE.VALIDER_PAR AS VALIDER_PAR,	
                T_COURRIER_AGENCE.Matricule AS Matricule,	
                T_COURRIER_AGENCE.DESTINATAIRE AS DESTINATAIRE,	
                T_COURRIER_AGENCE.VALID AS VALID
            FROM 
                T_COURRIER_AGENCE
            WHERE 
                T_COURRIER_AGENCE.SAISIE_PAR <> 0
            ORDER BY 
                DATE_HEURE_SAISIE DESC
        '''
        return query

    
    def Req_ls_decompte_cheque(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.NUM_DECOMPTE AS NUM_DECOMPTE,	
                T_DECOMPTE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_DECOMPTE.DATE_DECOMPTE AS DATE_DECOMPTE,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT,	
                T_DECOMPTE.CODE_CAISSE AS CODE_CAISSE,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_DECOMPTE.DATE_HEURE_VERS AS DATE_HEURE_VERS,	
                T_DECOMPTE.CODE_CLIENT AS CODE_CLIENT,	
                T_DECOMPTE.CODE_BANQUE AS CODE_BANQUE,	
                T_DECOMPTE.REFERENCE AS REFERENCE,	
                T_BANQUES.LIBELLE AS LIBELLE,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_DECOMPTE.REGLEMENT AS REGLEMENT
            FROM 
                T_BANQUES,	
                T_DECOMPTE,	
                T_CLIENTS
            WHERE 
                T_DECOMPTE.CODE_CLIENT = T_CLIENTS.CODE_CLIENT
                AND		T_BANQUES.NUM_BANQUE = T_DECOMPTE.CODE_BANQUE
                AND
                (
                    T_DECOMPTE.CODE_OPERATEUR = {Param_param_operateur}
                    AND	T_DECOMPTE.DATE_DECOMPTE = '{Param_date_decompte}'
                    AND	T_DECOMPTE.MODE_PAIEMENT = 'C'
                    AND	T_DECOMPTE.REGLEMENT = 0
                )
        '''

        try:
            kwargs = {
                'Param_param_operateur': args[0],
                'Param_date_decompte': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_decompte'] = self.validateDate(kwargs['Param_date_decompte'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_decompte_espece(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.NUM_DECOMPTE AS NUM_DECOMPTE,	
                T_DECOMPTE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_DECOMPTE.DATE_DECOMPTE AS DATE_DECOMPTE,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT,	
                T_DECOMPTE.CODE_CAISSE AS CODE_CAISSE,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_DECOMPTE.DATE_HEURE_VERS AS DATE_HEURE_VERS,	
                T_DECOMPTE.REGLEMENT AS REGLEMENT
            FROM 
                T_DECOMPTE
            WHERE 
                T_DECOMPTE.CODE_OPERATEUR = {Param_param_operateur}
                AND	T_DECOMPTE.DATE_DECOMPTE = '{Param_date_decompte}'
                AND	T_DECOMPTE.MODE_PAIEMENT = 'E'
                AND	T_DECOMPTE.REGLEMENT = 0
        '''

        try:
            kwargs = {
                'Param_param_operateur': args[0],
                'Param_date_decompte': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_decompte'] = self.validateDate(kwargs['Param_date_decompte'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_decompte_journee(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.NUM_DECOMPTE AS NUM_DECOMPTE,	
                T_DECOMPTE.DATE_DECOMPTE AS DATE_DECOMPTE,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT
            FROM 
                T_DECOMPTE
            WHERE 
                T_DECOMPTE.DATE_DECOMPTE = '{Param_date_decompte}'
        '''

        try:
            kwargs = {
                'Param_date_decompte': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_decompte'] = self.validateDate(kwargs['Param_date_decompte'])

        if kwargs['Param_date_decompte'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_depense_caisse(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.LIBELLE AS LIBELLE,	
                T_OPERATIONS_CAISSE.NUM_PIECE AS NUM_PIECE,	
                T_OPERATIONS_CAISSE.NATURE AS NATURE,	
                T_OPERATIONS_CAISSE.BENEFICIAIRE AS BENEFICIAIRE,	
                T_OPERATIONS_CAISSE.COMPTE_VERSEMENT AS COMPTE_VERSEMENT,	
                T_OPERATIONS_CAISSE.COMMENTAIRE AS COMMENTAIRE,	
                T_MOUVEMENTS_CAISSE.CODE_CAISSE AS CODE_CAISSE,	
                T_MOUVEMENTS_CAISSE.MONTANT AS MONTANT_T_
            FROM 
                T_OPERATIONS_CAISSE,	
                T_MOUVEMENTS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.CODE_OPERATION = T_MOUVEMENTS_CAISSE.ORIGINE
                AND
                (
                    T_MOUVEMENTS_CAISSE.CODE_CAISSE = {Param_code_caisse}
                    AND	T_OPERATIONS_CAISSE.DATE_OPERATION <= '{Param_dt}'
                    AND	
                    (
                        T_OPERATIONS_CAISSE.DATE_VALIDATION >= '{Param_dt}'
                        OR	T_OPERATIONS_CAISSE.DATE_VALIDATION = '1900-01-01 00:00:00'
                    )
                )
        '''

        try:
            kwargs = {
                'Param_code_caisse': args[0],
                'Param_dt': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_depenses(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.LIBELLE AS LIBELLE,	
                T_OPERATIONS_CAISSE.MONTANT AS MONTANT,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_MOUVEMENTS_CAISSE.CODE_CAISSE AS CODE_CAISSE,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.NATURE AS NATURE
            FROM 
                T_OPERATIONS_CAISSE,	
                T_MOUVEMENTS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.CODE_OPERATION = T_MOUVEMENTS_CAISSE.ORIGINE
                AND
                (
                    T_OPERATIONS_CAISSE.TYPE_OPERATION IN ('D', 'V') 
                    AND	T_MOUVEMENTS_CAISSE.CODE_CAISSE = {Param_code_caisse}
                    AND	T_OPERATIONS_CAISSE.DATE_VALIDATION = '{Param_date_operation}'
                )
        '''

        try:
            kwargs = {
                'Param_code_caisse': args[0],
                'Param_date_operation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_operation'] = self.validateDate(kwargs['Param_date_operation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_depositaires(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.ACTIF AS ACTIF,	
                T_OPERATEUR.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_OPERATEUR.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_OPERATEUR.CODE_INTERNE AS CODE_INTERNE
            FROM 
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.FONCTION = 11
                AND	T_OPERATEUR.ACTIF = 1
        '''
        return query

    
    def req_ls_ecarts_controleur(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.COMPTE_ECART AS COMPTE_ECART,	
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                SUM(T_MOUVEMENTS.QTE_ECART) AS la_somme_QTE_ECART,	
                SUM(T_MOUVEMENTS.MONTANT_ECART) AS la_somme_MONTANT_ECART,	
                T_MAGASINS.NOM_MAGASIN AS NOM_MAGASIN
            FROM 
                T_PRODUITS,	
                T_ARTICLES,	
                T_MOUVEMENTS,	
                T_MAGASINS
            WHERE 
                T_MAGASINS.CODE_MAGASIN = T_MOUVEMENTS.CODE_MAGASIN
                AND		T_ARTICLES.CODE_ARTICLE = T_MOUVEMENTS.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND
                (
                    T_MOUVEMENTS.COMPTE_ECART = {Param_compte_ecart}
                    AND	T_MOUVEMENTS.DATE_MVT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MAGASINS.NOM_MAGASIN,	
                T_MOUVEMENTS.COMPTE_ECART,	
                T_PRODUITS.NOM_PRODUIT
        '''

        try:
            kwargs = {
                'Param_compte_ecart': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def req_ls_ecarts_controleur_caisserie(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS.DATE_OPERATION AS DATE_OPERATION,	
                T_MOUVEMENTS_CAISSERIE.COMPTE_ECART AS COMPTE_ECART,	
                T_MOUVEMENTS_CAISSERIE.CODE_MAGASIN AS CODE_MAGASIN,	
                T_MOUVEMENTS_CAISSERIE.CODE_CP AS CODE_CP,	
                T_MOUVEMENTS_CAISSERIE.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MAGASINS.NOM_MAGASIN AS NOM_MAGASIN,	
                SUM(T_MOUVEMENTS_CAISSERIE.QTE_ECART) AS la_somme_QTE_ECART
            FROM 
                T_OPERATIONS,	
                T_MOUVEMENTS_CAISSERIE,	
                T_MAGASINS
            WHERE 
                T_OPERATIONS.CODE_OPERATION = T_MOUVEMENTS_CAISSERIE.ORIGINE
                AND		T_MAGASINS.CODE_MAGASIN = T_MOUVEMENTS_CAISSERIE.CODE_MAGASIN
                AND
                (
                    T_OPERATIONS.DATE_OPERATION BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_MOUVEMENTS_CAISSERIE.COMPTE_ECART = {Param_compte_ecart}
                )
            GROUP BY 
                T_OPERATIONS.DATE_OPERATION,	
                T_MOUVEMENTS_CAISSERIE.COMPTE_ECART,	
                T_MOUVEMENTS_CAISSERIE.CODE_MAGASIN,	
                T_MOUVEMENTS_CAISSERIE.CODE_CP,	
                T_MOUVEMENTS_CAISSERIE.TYPE_MOUVEMENT,	
                T_MAGASINS.NOM_MAGASIN
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_compte_ecart': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def req_ls_ecarts_journee(self, args): #Done
        query = '''
            SELECT 
                T_ECARTS_RENDUS.ID_ECART AS ID_ECART,	
                T_ECARTS_RENDUS.DATE_SITUATION AS DATE_SITUATION,	
                T_ECARTS_RENDUS.DATE_HEURE_SAISIE AS DATE_HEURE_SAISIE,	
                T_ECARTS_RENDUS.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_ECARTS_RENDUS.VALEUR_PRODUIT AS VALEUR_PRODUIT,	
                T_ECARTS_RENDUS.CS_STD AS CS_STD,	
                T_ECARTS_RENDUS.CS_PRM AS CS_PRM,	
                T_ECARTS_RENDUS.CS_AGR AS CS_AGR,	
                T_ECARTS_RENDUS.PAL_STD AS PAL_STD,	
                T_ECARTS_RENDUS.PAL_UHT AS PAL_UHT,	
                T_ECARTS_RENDUS.VALID AS VALID,	
                T_ECARTS_RENDUS.PAL_EURO AS PAL_EURO,	
                T_ECARTS_RENDUS.CS_BLC AS CS_BLC,	
                T_ECARTS_RENDUS.CS_1 AS CS_1,	
                T_ECARTS_RENDUS.CS_2 AS CS_2
            FROM 
                T_ECARTS_RENDUS
            WHERE 
                T_ECARTS_RENDUS.VALID = 1
                {OPTIONAL_ARG_1}
                AND	T_ECARTS_RENDUS.DATE_HEURE_SAISIE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
        '''

        try:
            kwargs = {
                'Param_date_situation': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            kwargs[key] = self.validateDate(kwargs[key])
        
        for key in ('Param_dt1', 'Param_dt2'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = '''AND T_ECARTS_RENDUS.DATE_SITUATION = '{Param_date_situation}' '''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_situation'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_ecarts_rendus(self, args): #Done
        query = '''
            SELECT 
                T_ECARTS_RENDUS.ID_ECART AS ID_ECART,	
                T_ECARTS_RENDUS.DATE_SITUATION AS DATE_SITUATION,	
                T_ECARTS_RENDUS.DATE_HEURE_SAISIE AS DATE_HEURE_SAISIE,	
                T_ECARTS_RENDUS.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_ECARTS_RENDUS.VALEUR_PRODUIT AS VALEUR_PRODUIT,	
                T_ECARTS_RENDUS.CS_STD AS CS_STD,	
                T_ECARTS_RENDUS.CS_PRM AS CS_PRM,	
                T_ECARTS_RENDUS.CS_AGR AS CS_AGR,	
                T_ECARTS_RENDUS.PAL_STD AS PAL_STD,	
                T_ECARTS_RENDUS.PAL_UHT AS PAL_UHT,	
                T_ECARTS_RENDUS.PAL_EURO AS PAL_EURO,	
                T_ECARTS_RENDUS.CS_BLC AS CS_BLC
            FROM 
                T_OPERATEUR,	
                T_ECARTS_RENDUS
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_ECARTS_RENDUS.CODE_OPERATEUR
                AND
                (
                    T_ECARTS_RENDUS.DATE_HEURE_SAISIE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_ls_enseigne_secteur(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_GROUP_CLIENTS.ID_GP_CLIENT AS ID_GP_CLIENT,	
                T_GROUP_CLIENTS.NOM_GROUP AS NOM_GROUP,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur
            FROM 
                T_SOUS_SECTEUR,	
                T_GROUP_CLIENTS,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.SOUS_SECTEUR = T_SOUS_SECTEUR.CODE_SOUS_SECTEUR
                AND		T_CLIENTS.GROUP_CLIENT = T_GROUP_CLIENTS.ID_GP_CLIENT
                AND
                (
                    T_SOUS_SECTEUR.code_secteur = {Param_CODE_SECTEUR}
                )
        '''
        
        try:
            kwargs = {
                'Param_CODE_SECTEUR': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_CODE_SECTEUR'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_facture_date(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.NUM_FACTURE AS NUM_FACTURE,	
                T_FACTURE.DATE_HEURE AS DATE_HEURE,	
                T_FACTURE.MONTANT_FACTURE AS MONTANT_FACTURE,	
                T_FACTURE.VALID AS VALID,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur
            FROM 
                T_FACTURE,	
                T_CLIENTS,	
                T_SOUS_SECTEUR
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_FACTURE.CODE_CLIENT = T_CLIENTS.CODE_CLIENT
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_SOUS_SECTEUR.code_secteur = {Param_CODE_SECTEUR}
                )
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_CODE_SECTEUR': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
    
        return query.format(**kwargs)

    
    def Req_ls_factures_clients(self, args):
        query = '''
            SELECT 
                T_FACTURE.NUM_FACTURE AS NUM_FACTURE,	
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_FACTURE.MONTANT_FACTURE AS MONTANT_FACTURE,	
                T_FACTURE.MONTANT_PERTE AS MONTANT_PERTE,	
                T_FACTURE.VALID AS VALID,	
                T_FACTURE.DATE_HEURE AS DATE_HEURE,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.CLASSE AS CLASSE,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_ZONE.CODE_SUPERVISEUR AS CODE_SUPERVISEUR,	
                T_ZONE.RESP_VENTE AS RESP_VENTE,	
                SUM(( ( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) * T_DT_FACTURE.PRIX ) * T_DT_FACTURE.TX_GRATUIT ) /  100) ) AS la_somme_Gratuit
            FROM 
                T_ZONE,	
                T_BLOC,	
                T_SECTEUR,	
                T_SOUS_SECTEUR,	
                T_CLIENTS,	
                T_FACTURE,	
                T_OPERATEUR,	
                T_DT_FACTURE
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_FACTURE.CODE_OPERATEUR
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_DT_FACTURE.NUM_FACTURE = T_FACTURE.NUM_FACTURE
                AND
                (
                    T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
                    AND	T_FACTURE.DATE_HEURE BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_FACTURE.CODE_CLIENT = {Param_code_client}
                    AND	T_FACTURE.VALID = 1
                    AND	T_CLIENTS.CLASSE = {Param_classe}
                    AND	T_CLIENTS.CLIENT_EN_COMPTE = {Param_cac}
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_ZONE.RESP_VENTE = {Param_resp_vente}
                )
            GROUP BY 
                T_FACTURE.NUM_FACTURE,	
                T_FACTURE.CODE_CLIENT,	
                T_FACTURE.MONTANT_FACTURE,	
                T_FACTURE.MONTANT_PERTE,	
                T_FACTURE.VALID,	
                T_FACTURE.DATE_HEURE,	
                T_SOUS_SECTEUR.code_secteur,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_CLIENTS.NOM_CLIENT,	
                T_CLIENTS.CLASSE,	
                T_CLIENTS.CLIENT_EN_COMPTE,	
                T_CLIENTS.CAT_CLIENT,	
                T_ZONE.CODE_SUPERVISEUR,	
                T_ZONE.RESP_VENTE
        '''
        return query.format(**kwargs)

    
    def Req_ls_gms(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT
            FROM 
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT <> 0
                AND	
                (
                    T_CLIENTS.CAT_CLIENT = 1
                    OR	T_CLIENTS.CAT_CLIENT = 15
                )
            ORDER BY 
                NOM_CLIENT ASC
        '''
        return query

    
    def Req_ls_gms_depo(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT
            FROM 
                T_CLIENTS
            WHERE 
                T_CLIENTS.CAT_CLIENT IN (1, 2) 
                AND	T_CLIENTS.CODE_CLIENT <> 0
            ORDER BY 
                NOM_CLIENT ASC
        '''
        return query

    
    def Req_ls_groups(self, args): #Done
        query = '''
            SELECT 
                T_GROUP_CLIENTS.ID_GP_CLIENT AS ID_GP_CLIENT,	
                T_GROUP_CLIENTS.NOM_GROUP AS NOM_GROUP
            FROM 
                T_GROUP_CLIENTS
            ORDER BY 
                NOM_GROUP ASC
        '''
        return query

    
    def Req_ls_liv(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.STATUT AS STATUT,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT
            FROM 
                T_LIVRAISON
            WHERE 
                T_LIVRAISON.STATUT <> 'A'
                AND	T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_livraison(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_LIVRAISON.code_secteur AS code_secteur,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT
            FROM 
                T_CLIENTS,	
                T_LIVRAISON
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND
                (
                    T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_LIVRAISON.code_secteur = {Param_code_secteur}
                )
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_livraison_client(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.SOUS_SECTEUR AS SOUS_SECTEUR,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT
            FROM 
                T_SOUS_SECTEUR,	
                T_CLIENTS,	
                T_OPERATIONS
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_OPERATIONS.CODE_CLIENT
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
                )
        '''
        
        try:
            kwargs = {
                'Param_code_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_livraison_tournee_journee(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.code_secteur AS code_secteur,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.RANG AS RANG,	
                T_LIVRAISON.NUM_COMMANDE AS NUM_COMMANDE,	
                T_LIVRAISON.SUR_COMMANDE AS SUR_COMMANDE
            FROM 
                T_CLIENTS,	
                T_LIVRAISON
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND
                (
                    T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_LIVRAISON.code_secteur = {Param_code_secteur}
                    {OPTIONAL_ARG_1}
                    AND	T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.SUR_COMMANDE = 1
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_secteur': args[1],
                'Param_code_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in ('Param_date_livraison', 'Param_code_secteur'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_LIVRAISON.CODE_CLIENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_livraisons(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_LIVRAISON.code_secteur AS code_secteur,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_LIVRAISON.code_vendeur AS code_vendeur,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_LIVRAISON.vehicule AS vehicule,	
                T_LIVRAISON.Type_Livraison AS Type_Livraison
            FROM 
                T_OPERATEUR,	
                T_LIVRAISON,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_LIVRAISON.code_vendeur
                AND
                (
                    T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_LIVRAISON.Type_Livraison IN (1, 2) 
                )
        '''
        
        try:
            kwargs = {
                'Param_date_livraison': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_livraisons_journee(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_LIVRAISON.code_secteur AS code_secteur,	
                T_LIVRAISON.Type_Livraison AS Type_Livraison,	
                T_LIVRAISON.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_LIVRAISON.DATE_HEURE AS DATE_HEURE,	
                T_LIVRAISON.code_vendeur AS code_vendeur,	
                T_LIVRAISON.STATUT AS STATUT,	
                T_LIVRAISON.LIVRAISON_TOURNEE AS LIVRAISON_TOURNEE,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_LIVRAISON.BENEFICIAIRE AS BENEFICIAIRE,	
                T_LIVRAISON.ORDONATEUR AS ORDONATEUR,	
                T_LIVRAISON.NUM_COMMANDE AS NUM_COMMANDE,	
                T_CLIENTS.TYPE_BL AS TYPE_BL,	
                T_CLIENTS.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE,	
                T_CLIENTS.SOLDE_C_PR AS SOLDE_C_PR,	
                T_CLIENTS.GROUP_CLIENT AS GROUP_CLIENT
            FROM 
                T_CLIENTS,	
                T_LIVRAISON,	
                T_SECTEUR
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND		T_LIVRAISON.code_secteur = T_SECTEUR.code_secteur
                AND
                (
                    T_LIVRAISON.DATE_LIVRAISON = '{Param_date_chargement}'
                    AND	T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.TYPE_MVT = {Param_type_mvt}
                )
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_type_mvt': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_motifs(self, args): #Done
        query = '''
            SELECT 
                T_MOTIF.CODE_MOTIF AS CODE_MOTIF,	
                T_MOTIF.MOTIF AS MOTIF,	
                T_MOTIF.MOTIF_TRANSFERT AS MOTIF_TRANSFERT,	
                T_MOTIF.MOTIF_RETOUR_CAC AS MOTIF_RETOUR_CAC
            FROM 
                T_MOTIF
            WHERE 
                T_MOTIF.MOTIF_TRANSFERT = {Param_transfert}
                AND	T_MOTIF.MOTIF_RETOUR_CAC = {Param_retour_cac}
        '''

        try:
            kwargs = {
                'Param_transfert': args[0],
                'Param_retour_cac': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def req_ls_mvt(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_MAGASINS.NOM_MAGASIN AS NOM_MAGASIN,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT,	
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT
            FROM 
                T_MAGASINS,	
                T_MOUVEMENTS,	
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_MOUVEMENTS.CODE_ARTICLE
                AND		T_MAGASINS.CODE_MAGASIN = T_MOUVEMENTS.CODE_MAGASIN
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                )
        '''

        try:
            kwargs = {
                'Param_code_produit': args[0],
                'Param_date_mvt': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_MOUVEMENTS.CODE_ARTICLE = {Param_code_produit} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_produit'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_num_bl_client(self, args): #Done
        query = '''
            SELECT TOP 10 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_LIVRAISON.STATUT AS STATUT,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT
            FROM 
                T_LIVRAISON
            WHERE 
                T_LIVRAISON.STATUT <> 'A'
                AND	T_LIVRAISON.TYPE_MVT = 'L'
                AND	T_LIVRAISON.CODE_CLIENT = {Param_code_client}
            ORDER BY 
                NUM_LIVRAISON DESC
        '''
        
        try:
            kwargs = {
                'Param_code_client': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_client'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_operateurs(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.CODE_AGCE AS CODE_AGCE,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.MDP AS MDP,	
                T_OPERATEUR.TITULAIRE AS TITULAIRE,	
                T_OPERATEUR.ACTIF AS ACTIF,	
                T_OPERATEUR.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_OPERATEUR.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_FONCTION.NOM_FONCTION AS NOM_FONCTION,	
                T_OPERATEUR.SOLDE_C_STD AS SOLDE_C_STD,	
                T_OPERATEUR.SOLDE_P_AG AS SOLDE_P_AG,	
                T_OPERATEUR.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_OPERATEUR.SOLDE_C_AG AS SOLDE_C_AG,	
                T_OPERATEUR.SOLDE_C_PR AS SOLDE_C_PR,	
                T_OPERATEUR.CODE_INTERNE AS CODE_INTERNE
            FROM 
                T_FONCTION,	
                T_OPERATEUR
            WHERE 
                T_FONCTION.CODE_FONCTION = T_OPERATEUR.FONCTION
                AND
                (
                    T_OPERATEUR.ACTIF = {Param_actif}
                    AND	T_OPERATEUR.CODE_OPERATEUR <> 0
                )
            ORDER BY 
                NOM_OPERATEUR ASC
        '''
        
        try:
            kwargs = {
                'Param_actif': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_actif'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_operateurs_fonction(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_FONCTION.NOM_FONCTION AS NOM_FONCTION,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.ACTIF AS ACTIF,	
                T_OPERATEUR.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_OPERATEUR.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_OPERATEUR.SOLDE_C_STD AS SOLDE_C_STD,	
                T_OPERATEUR.SOLDE_P_AG AS SOLDE_P_AG,	
                T_OPERATEUR.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_OPERATEUR.SOLDE_C_AG AS SOLDE_C_AG,	
                T_OPERATEUR.SOLDE_C_PR AS SOLDE_C_PR
            FROM 
                T_FONCTION,	
                T_OPERATEUR
            WHERE 
                T_FONCTION.CODE_FONCTION = T_OPERATEUR.FONCTION
                AND
                (
                    T_OPERATEUR.FONCTION IN ({Param_fonction}) 
                    AND	T_OPERATEUR.ACTIF = 1
                )
            ORDER BY 
                NOM_OPERATEUR ASC
        '''
        
        try:
            kwargs = {
                'Param_fonction': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_fonction'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_operation_caisse_valide(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION
            FROM 
                T_OPERATIONS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.DATE_VALIDATION = '{Param_date_validation}'
        '''

        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_operations(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_OPERATIONS.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS.DATE_HEURE AS DATE_HEURE,	
                T_OPERATIONS.TYPE_PRODUIT AS TYPE_PRODUIT,	
                T_OPERATIONS.CODE_AGCE1 AS CODE_AGCE1,	
                T_OPERATIONS.COMMENTAIRE AS COMMENTAIRE,	
                T_OPERATIONS.NUM_CONVOYAGE AS NUM_CONVOYAGE,	
                T_OPERATIONS.MOTIF AS MOTIF,	
                T_OPERATIONS.REF AS REF,	
                T_OPERATIONS.CODE_MAGASIN1 AS CODE_MAGASIN,	
                T_OPERATIONS.COMPTE_ECART AS COMPTE_ECART,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATIONS.CATEGORIE1 AS CATEGORIE1,	
                T_OPERATIONS.CATEGORIE2 AS CATEGORIE2,	
                T_OPERATIONS.SOUS_TYPE_OPERATION AS SOUS_TYPE_OPERATION,	
                OP.NOM_OPERATEUR AS OP_SAISIE,	
                T_OPERATIONS.CODE_AGCE2 AS CODE_AGCE2,	
                T_Ordre_Mission_Agence.Matricule_Vehicule AS Matricule,	
                T_Ordre_Mission_Agence.Chauffeurs_Mission AS Chauffeur,	
                T_Ordre_Mission_Agence.Nom_Transporteur AS PROPRIETAIRE
            FROM 
                T_Ordre_Mission_Agence,	
                T_OPERATIONS,	
                T_OPERATEUR,	
                T_OPERATEUR OP
            WHERE 
                T_OPERATIONS.COMPTE_ECART = T_OPERATEUR.CODE_OPERATEUR
                AND		OP.CODE_OPERATEUR = T_OPERATIONS.CODE_OPERATEUR
                AND		T_Ordre_Mission_Agence.Id_Ordre_Mission = T_OPERATIONS.NUM_CONVOYAGE
                AND
                {CODE_BLOCK_1}
        '''

        try:
            kwargs = {
                'Param_code_agce': args[0],
                'Param_type_operation': args[1],
                'Param_date_operation': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_operation'] = self.validateDate(kwargs['Param_date_operation'])
        kwargs['CODE_BLOCK_1'] = '''AND
                (
                    {OPTIONAL_ARG_1}	
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                )'''
        kwargs['OPTIONAL_ARG_1'] = 'T_OPERATIONS.CODE_AGCE1 = {Param_code_agce} AND'
        kwargs['OPTIONAL_ARG_2'] = 'T_OPERATIONS.TYPE_OPERATION = {Param_type_operation} AND'
        kwargs['OPTIONAL_ARG_3'] = '''T_OPERATIONS.DATE_OPERATION = '{Param_date_operation}' '''

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_agce'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_type_operation'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['Param_date_operation'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']

        return query.format(**kwargs).format(**kwargs).format(**kwargs)

    
    def Req_ls_operations_caisse(self, args):
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.NUM_PIECE AS NUM_PIECE,	
                T_OPERATIONS_CAISSE.NATURE AS NATURE,	
                T_OPERATIONS_CAISSE.BENEFICIAIRE AS BENEFICIAIRE,	
                T_OPERATIONS_CAISSE.COMPTE_VERSEMENT AS COMPTE_VERSEMENT,	
                T_OPERATIONS_CAISSE.COMMENTAIRE AS COMMENTAIRE,	
                T_OPERATIONS_CAISSE.MONTANT AS MONTANT,	
                T_OPERATIONS_CAISSE.LIBELLE AS LIBELLE,	
                T_MOUVEMENTS_CAISSE.MONTANT AS MONTANT_MVT,	
                T_MOUVEMENTS_CAISSE.CODE_CAISSE AS CODE_CAISSE,	
                T_CAISSE.NOM_CAISSE AS NOM_CAISSE,	
                T_MOUVEMENTS_CAISSE.TYPE_MVT AS TYPE_MVT
            FROM 
                T_OPERATIONS_CAISSE,	
                T_MOUVEMENTS_CAISSE,	
                T_CAISSE
            WHERE 
                T_CAISSE.CODE_CAISSE = T_MOUVEMENTS_CAISSE.CODE_CAISSE
                AND		T_OPERATIONS_CAISSE.CODE_OPERATION = T_MOUVEMENTS_CAISSE.ORIGINE
                AND
                (
                    T_OPERATIONS_CAISSE.DATE_OPERATION BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_OPERATIONS_CAISSE.DATE_VALIDATION = {Param_date_validation}
                    AND	T_OPERATIONS_CAISSE.TYPE_OPERATION IN ({Param_type_operation}) 
                    AND	T_OPERATIONS_CAISSE.DATE_VALIDATION <> ({Param_diff_validation}) 
                )
            ORDER BY 
                CODE_OPERATION DESC
        '''
        return query.format(**kwargs)

    
    def Req_ls_operations_non_justifies(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.NUM_PIECE AS NUM_PIECE,	
                T_OPERATIONS_CAISSE.NATURE AS NATURE,	
                T_OPERATIONS_CAISSE.BENEFICIAIRE AS BENEFICIAIRE,	
                T_OPERATIONS_CAISSE.COMPTE_VERSEMENT AS COMPTE_VERSEMENT,	
                T_OPERATIONS_CAISSE.COMMENTAIRE AS COMMENTAIRE,	
                T_OPERATIONS_CAISSE.MONTANT AS MONTANT,	
                T_OPERATIONS_CAISSE.LIBELLE AS LIBELLE,	
                T_MOUVEMENTS_CAISSE.MONTANT AS MONTANT_MVT,	
                T_MOUVEMENTS_CAISSE.CODE_CAISSE AS CODE_CAISSE,	
                T_CAISSE.NOM_CAISSE AS NOM_CAISSE,	
                T_MOUVEMENTS_CAISSE.TYPE_MVT AS TYPE_MVT
            FROM 
                T_OPERATIONS_CAISSE,	
                T_MOUVEMENTS_CAISSE,	
                T_CAISSE
            WHERE 
                T_CAISSE.CODE_CAISSE = T_MOUVEMENTS_CAISSE.CODE_CAISSE
                AND		T_OPERATIONS_CAISSE.CODE_OPERATION = T_MOUVEMENTS_CAISSE.ORIGINE
                AND
                (
                    T_OPERATIONS_CAISSE.DATE_OPERATION = '{Param_date_operation}'
                    AND	T_OPERATIONS_CAISSE.DATE_VALIDATION = '1900-01-01 00:00:00'
                    AND	T_OPERATIONS_CAISSE.TYPE_OPERATION IN ({Param_type_operation}) 
                )
        '''

        try:
            kwargs = {
                'Param_date_operation': args[0],
                'Param_type_operation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_operation'] = self.validateDate(kwargs['Param_date_operation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_prelev_journee(self, args): #Done
        query = '''
            SELECT 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV AS ID_PRELEV,	
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION AS DATE_VALIDATION
            FROM 
                T_PRELEVEMENT_SUSP_COND
            WHERE 
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION = '{Param_dt}'
        '''

        try:
            kwargs = {
                'Param_dt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])

        if kwargs['Param_dt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_prelevement(self, args): #Done
        query = '''
            SELECT 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV AS ID_PRELEV,	
                T_PRELEVEMENT_SUSP_COND.DATE_PRELEV AS DATE_PRELEV,	
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION AS DATE_VALIDATION
            FROM 
                T_PRELEVEMENT_SUSP_COND
            ORDER BY 
                ID_PRELEV DESC
        '''
        return query

    
    def Req_ls_prelevements_periode(self, args): #Done
        query = '''
            SELECT 
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION AS DATE_VALIDATION,	
                SUM(T_DT_PRELEVEMENT_COND.SUSP_VENTE) AS la_somme_SUSP_VENTE
            FROM 
                T_PRELEVEMENT_SUSP_COND,	
                T_DT_PRELEVEMENT_COND
            WHERE 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV = T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT
                AND
                (
                    T_DT_PRELEVEMENT_COND.CODE_OPERATEUR = {Param_code_operateur}
                    AND	T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR,	
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_prelevements_periode_caisserie(self, args): #Done
        query = '''
            SELECT 
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION AS DATE_VALIDATION,	
                SUM(( T_DT_PRELEVEMENT_COND.SOLDE_STD - T_DT_PRELEVEMENT_COND.DECON_STD ) ) AS la_somme_SOLDE_STD,	
                SUM(( T_DT_PRELEVEMENT_COND.SOLDE_AGR - T_DT_PRELEVEMENT_COND.DECON_AGR ) ) AS la_somme_SOLDE_AGR,	
                SUM(( T_DT_PRELEVEMENT_COND.SOLDE_PRM - T_DT_PRELEVEMENT_COND.DECON_PRM ) ) AS la_somme_SOLDE_PRM,	
                SUM(( T_DT_PRELEVEMENT_COND.SOLDE_PAG - T_DT_PRELEVEMENT_COND.DECON_PAG ) ) AS la_somme_SOLDE_PAG,	
                SUM(( T_DT_PRELEVEMENT_COND.SOLDE_PUHT - T_DT_PRELEVEMENT_COND.DECON_PUHT ) ) AS la_somme_SOLDE_PUHT,	
                SUM(( T_DT_PRELEVEMENT_COND.SOLDE_EURO - T_DT_PRELEVEMENT_COND.DECON_EURO ) ) AS la_somme_SOLDE_EURO,	
                SUM(( T_DT_PRELEVEMENT_COND.SOLDE_CBL - T_DT_PRELEVEMENT_COND.DECON_CBL ) ) AS la_somme_SOLDE_CBL
            FROM 
                T_PRELEVEMENT_SUSP_COND,	
                T_DT_PRELEVEMENT_COND
            WHERE 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV = T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT
                AND
                (
                    T_DT_PRELEVEMENT_COND.CODE_OPERATEUR = {Param_code_operateur}
                    AND	T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR,	
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_preparation(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.RANG AS RANG,	
                T_PREPARATION_CHARGEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PREPARATION_CHARGEMENTS.IMPORTE AS IMPORTE,	
                T_PREPARATION_CHARGEMENTS.SORTIE1 AS SORTIE1,	
                T_PREPARATION_CHARGEMENTS.SORTIE2 AS SORTIE2,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.QTE_PACK AS QTE_PACK,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_PREPARATION_CHARGEMENTS.CHARG_GMS AS CHARG_GMS,	
                T_PREPARATION_CHARGEMENTS.REST_STOCK AS REST_STOCK
            FROM 
                T_ARTICLES,	
                T_PREPARATION_CHARGEMENTS
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PREPARATION_CHARGEMENTS.CODE_ARTICLE

            ORDER BY 
                RANG ASC
        '''
        return query

    
    def Req_ls_prix_produit(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_PRIX.PRIX AS PRIX_VENTE
            FROM 
                T_ARTICLES,	
                T_PRIX,	
                T_PRODUITS
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND
                (
                    T_PRIX.Date_Debut <= '{param_dt}'
                    AND	T_PRIX.Date_Fin >= '{param_dt}'
                )
        '''

        try:
            kwargs = {
                'param_dt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['param_dt'] = self.validateDate(kwargs['param_dt'])

        if kwargs['param_dt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_produit_commandes(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_COMMANDES.ID_COMMANDE AS ID_COMMANDE,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_COMMANDES.QTE_U AS QTE_U,	
                T_PRODUITS_COMMANDES.QTE_C AS QTE_C,	
                T_PRODUITS_COMMANDES.QTE_P AS QTE_P,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_PRIX_AGENCE.PRIX_VENTE AS PRIX_VENTE,	
                T_PRIX_AGENCE.CODE_AGCE AS CODE_AGCE,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.CODE_BARRE AS CODE_BARRE
            FROM 
                T_ARTICLES,	
                T_PRIX_AGENCE,	
                T_PRODUITS_COMMANDES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRODUITS_COMMANDES.CODE_ARTICLE
                AND		T_ARTICLES.CODE_ARTICLE = T_PRIX_AGENCE.CODE_ARTICLE
                AND
                (
                    T_PRODUITS_COMMANDES.ID_COMMANDE = {Param_id_commande}
                    AND	T_PRIX_AGENCE.CODE_AGCE = {Param_code_agce}
                )
        '''

        try:
            kwargs = {
                'Param_id_commande': args[0],
                'Param_code_agce': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_produits(self, args):
        query = '''
            SELECT DISTINCT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.QTE_PALETTE AS QTE_PALETTE,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_ARTICLES_MAGASINS.MAGASIN AS MAGASIN,	
                T_PRIX.PRIX AS PRIX_VENTE,	
                T_PRIX.CODE_AGCE AS CODE_AGCE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                T_ARTICLES.TYPE_CAISSE AS TYPE_CAISSE,	
                T_ARTICLES.TYPE_PALETTE AS TYPE_PALETTE,	
                T_ARTICLES.CONDITIONNEMENT2 AS CONDITIONNEMENT2,	
                T_ARTICLES.QTE_PALETTE2 AS QTE_PALETTE2,	
                T_ARTICLES.CODE_BARRE AS CODE_BARRE
            FROM 
                T_ARTICLES_MAGASINS,	
                T_PRIX,	
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND		T_ARTICLES.CODE_ARTICLE = T_ARTICLES_MAGASINS.CODE_ARTICLE
                AND
                (
                    T_ARTICLES.ACTIF = 1
                    AND	T_ARTICLES_MAGASINS.MAGASIN = {Param_code_mag}
                    AND	T_PRIX.CODE_AGCE = {Param_code_agce}
                    AND	T_PRIX.Date_Debut <= {param_dt}
                    AND	T_PRIX.Date_Fin >= {param_dt}
                    AND	T_ARTICLES.CODE_PRODUIT = {Param_code_produit}
                )
            ORDER BY 
                RANG ASC
        '''
        return query.format(**kwargs)

    
    def Req_ls_produits_actif(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                MAX(T_ARTICLES.RANG) AS le_maximum_RANG,	
                T_FAMILLE.NOM_FAMILLE AS NOM_FAMILLE,	
                T_FAMILLE.CODE_FAMILLE AS CODE_FAMILLE,	
                T_GAMME.CODE_GAMME AS CODE_GAMME,	
                T_GAMME.NOM_GAMME AS NOM_GAMME,	
                MAX(T_PRIX.PRIX) AS le_maximum_PRIX_VENTE,	
                MIN(T_ARTICLES.AFF_COMMANDE) AS le_minimum_AFF_COMMANDE
            FROM 
                T_ARTICLES,	
                T_PRIX,	
                T_PRODUITS,	
                T_FAMILLE,	
                T_GAMME
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_FAMILLE.CODE_FAMILLE = T_PRODUITS.CODE_FAMILLE
                AND		T_GAMME.CODE_GAMME = T_FAMILLE.CODE_GAMME
                AND
                (
                    T_PRIX.Date_Debut <= '{param_dt}'
                    AND	T_PRIX.Date_Fin >= '{param_dt}'
                    AND	T_ARTICLES.ACTIF = 1
                )
            GROUP BY 
                T_PRODUITS.CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT,	
                T_FAMILLE.NOM_FAMILLE,	
                T_FAMILLE.CODE_FAMILLE,	
                T_GAMME.CODE_GAMME,	
                T_GAMME.NOM_GAMME
            ORDER BY 
                le_maximum_RANG ASC
        '''

        try:
            kwargs = {
                'param_dt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['param_dt'] = self.validateDate(kwargs['param_dt'])

        if kwargs['param_dt'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)

    
    def Req_ls_produits_chargees(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.CODE_CHARGEMENT AS CODE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_VAL AS QTE_CHARGEE_VAL,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE AS QTE_CHARGEE_POINTE,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_SUPP AS QTE_CHARGEE_SUPP,	
                T_PRODUITS_CHARGEE.TOTAL_CHARGEE AS TOTAL_CHARGEE,	
                T_PRODUITS_CHARGEE.QTE_ECART AS QTE_ECART,	
                T_PRODUITS_CHARGEE.TOTAL_VENDU AS TOTAL_VENDU,	
                T_PRODUITS_CHARGEE.TOTAL_GRATUIT AS TOTAL_GRATUIT,	
                T_PRODUITS_CHARGEE.TOTAL_ECHANGE AS TOTAL_ECHANGE,	
                T_PRODUITS_CHARGEE.TOTAL_REMISE AS TOTAL_REMISE,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_POINTE AS TOTAL_RENDUS_POINTE,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG AS TOTAL_RENDUS_AG,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_US AS TOTAL_RENDUS_US,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM AS TOTAL_RENDUS_COM,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP AS TOTAL_RENDUS_SP,	
                T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE AS TOTAL_INVENDU_POINTE,	
                T_PRODUITS_CHARGEE.PRIX AS PRIX,	
                T_PRODUITS_CHARGEE.MONTANT AS MONTANT,	
                T_PRODUITS_CHARGEE.CREDIT AS CREDIT,	
                T_PRODUITS_CHARGEE.MONTANT_CREDIT AS MONTANT_CREDIT,	
                T_PRODUITS_CHARGEE.code_vendeur AS code_vendeur,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE AS QTE_CHARGEE,	
                T_PRODUITS_CHARGEE.MONTANT_ECART AS MONTANT_ECART,	
                T_PRODUITS_CHARGEE.COMPTE_ECART AS COMPTE_ECART,	
                T_PRODUITS_CHARGEE.TOTAL_DONS AS TOTAL_DONS,	
                T_PRODUITS_CHARGEE.PRIX_VNT AS PRIX_VNT
            FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.CODE_CHARGEMENT = {Param_code_chagement}
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_code_chagement': args[0],
                'Param_code_article': args[1]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_code_chagement'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_PRODUITS_CHARGEE.CODE_ARTICLE = {Param_code_article}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_article'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_produits_livraison(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_LIVREES.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_LIVREES.QTE_CHARGEE AS QTE_CHARGEE,	
                T_PRODUITS_LIVREES.MONTANT AS MONTANT,	
                T_PRODUITS_LIVREES.PRIX AS PRIX,	
                T_PRODUITS_LIVREES.QTE_CAISSE AS QTE_CAISSE,	
                T_PRODUITS_LIVREES.QTE_PAL AS QTE_PAL,	
                T_ARTICLES.CONDITIONNEMENT AS CONDITIONNEMENT,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.QTE_PALETTE AS QTE_PALETTE,	
                T_PRODUITS_LIVREES.MOTIF_RETOUR AS MOTIF_RETOUR,	
                T_MOTIFS_RETOUR.MOTIF AS MOTIF,	
                T_PRODUITS_LIVREES.CODE_MAGASIN AS CODE_MAGASIN,	
                T_PRODUITS_LIVREES.QTE_IMPORTE AS QTE_IMPORTE,	
                T_ARTICLES.RANG AS RANG,	
                T_ARTICLES.CODE_BARRE AS CODE_BARRE,	
                T_PRODUITS_LIVREES.QTE_COMMANDE AS QTE_COMMANDE,	
                T_ARTICLES.TYPE_CAISSE AS TYPE_CAISSE,	
                T_ARTICLES.TYPE_PALETTE AS TYPE_PALETTE,	
                T_ARTICLES.TVA AS TVA,	
                T_FAMILLE.CODE_GAMME AS CODE_GAMME
            FROM 
                T_MOTIFS_RETOUR,	
                T_ARTICLES,	
                T_PRODUITS_LIVREES,	
                T_PRODUITS,	
                T_FAMILLE
            WHERE 
                T_PRODUITS.CODE_FAMILLE = T_FAMILLE.CODE_FAMILLE
                AND		T_ARTICLES.CODE_PRODUIT = T_PRODUITS.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_LIVREES.CODE_ARTICLE
                AND		T_MOTIFS_RETOUR.CODE_MOTIF = T_PRODUITS_LIVREES.MOTIF_RETOUR
                AND
                (
                    T_PRODUITS_LIVREES.NUM_LIVRAISON = {Param_num_livraison}
                )
            ORDER BY 
                RANG ASC
        '''
        
        try:
            kwargs = {
                'Param_num_livraison': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_num_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_reclamations(self, args): #Done
        query = '''
            SELECT 
                T_RECLAMATIONS_QUALITE.ID_RECLAMATION AS ID_RECLAMATION,	
                T_RECLAMATIONS_QUALITE.DATE_HEURE AS DATE_HEURE,	
                T_RECLAMATIONS_QUALITE.Description AS Description,	
                T_RECLAMATIONS_QUALITE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_RECLAMATIONS_QUALITE.DLC AS DLC,	
                T_RECLAMATIONS_QUALITE.QTE_NC AS QTE_NC,	
                T_RECLAMATIONS_QUALITE.TYPE AS TYPE,	
                T_RECLAMATIONS_QUALITE.ORIGINE AS ORIGINE,	
                T_RECLAMATIONS_QUALITE.NBRE_CLIENTS AS NBRE_CLIENTS,	
                T_RECLAMATIONS_QUALITE.DATE_RECLAMATION AS DATE_RECLAMATION,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_ARTICLES.LIBELLE AS LIBELLE
            FROM 
                T_OPERATEUR,	
                T_RECLAMATIONS_QUALITE,	
                T_ARTICLES
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_RECLAMATIONS_QUALITE.EMETTEUR
                AND		T_ARTICLES.CODE_ARTICLE = T_RECLAMATIONS_QUALITE.CODE_ARTICLE
                AND
                (
                    T_RECLAMATIONS_QUALITE.DATE_RECLAMATION BETWEEN '{Param_date1}' AND '{Param_date2}'
                    AND	T_RECLAMATIONS_QUALITE.DATE_HEURE BETWEEN '{Param_dts1}' AND '{Param_dts2}'
                )
        '''

        try:
            kwargs = {
                'Param_date1': args[0],
                'Param_date2': args[1],
                'Param_dts1': args[2],
                'Param_dts2': args[3]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date1'] = self.validateDate(kwargs['Param_date1'])
        kwargs['Param_date2'] = self.validateDate(kwargs['Param_date2'])
        kwargs['Param_dts1'] = self.validateDate(kwargs['Param_dts1'])
        kwargs['Param_dts2'] = self.validateDate(kwargs['Param_dts2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_reconaissances(self, args): #Done
        query = '''
            SELECT 
                T_RECONAISSANCES.CODE_CLIENT AS CODE_CLIENT,	
                T_RECONAISSANCES.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_RECONAISSANCES.DATE_RECONAISS AS DATE_RECONAISS,	
                T_RECONAISSANCES.SOLDE_C_STD AS SOLDE_C_STD,	
                T_RECONAISSANCES.SOLDE_P_AG AS SOLDE_P_AG,	
                T_RECONAISSANCES.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_RECONAISSANCES.SOLDE_C_AG AS SOLDE_C_AG,	
                T_RECONAISSANCES.SOLDE_C_PR AS SOLDE_C_PR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_RECONAISSANCES.ID_RECONAISSANCE AS ID_RECONAISSANCE,	
                T_RECONAISSANCES.SOLDE_C_BLC AS SOLDE_C_BLC,	
                T_RECONAISSANCES.SOLDE_P_EURO AS SOLDE_P_EURO
            FROM 
                T_OPERATEUR,	
                T_RECONAISSANCES,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_RECONAISSANCES.CODE_CLIENT
                AND	T_OPERATEUR.CODE_OPERATEUR = T_RECONAISSANCES.CODE_OPERATEUR
        '''
        return query

    
    def Req_ls_reglement(self, args):
        query = '''
            SELECT 
                T_DECOMPTE.NUM_DECOMPTE AS NUM_DECOMPTE,	
                T_DECOMPTE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_DECOMPTE.REGLEMENT AS REGLEMENT,	
                T_DECOMPTE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_DECOMPTE.REFERENCE AS REFERENCE,	
                T_DECOMPTE.DATE_HEURE_VERS AS DATE_HEURE_VERS
            FROM 
                T_OPERATEUR,	
                T_DECOMPTE
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_DECOMPTE.CODE_OPERATEUR
                AND
                (
                    T_DECOMPTE.REGLEMENT = {Param_reglement}
                    AND	T_DECOMPTE.DATE_VALIDATION BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_DECOMPTE.CODE_OPERATEUR = {Param_code_operateur}
                    AND	T_DECOMPTE.MODE_PAIEMENT <> 'R'
                )
        '''
        return query.format(**kwargs)

    
    def Req_ls_reglements(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.DATE_DECOMPTE AS DATE_DECOMPTE,	
                T_DECOMPTE.MONTANT AS MONTANT,	
                T_DECOMPTE.DATE_HEURE_VERS AS DATE_HEURE_VERS,	
                T_DECOMPTE.REGLEMENT AS REGLEMENT,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_DECOMPTE.NUM_DECOMPTE AS NUM_DECOMPTE
            FROM 
                T_OPERATEUR,	
                T_DECOMPTE
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_DECOMPTE.CODE_OPERATEUR
                AND
                (
                    T_DECOMPTE.REGLEMENT = 1
                    AND	T_DECOMPTE.DATE_DECOMPTE = {Param_dt_reglement}
                )
        '''
        
        try:
            kwargs = {
                'Param_dt_reglement': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt_reglement'] = self.validateDate(kwargs['Param_dt_reglement'])

        if kwargs['Param_dt_reglement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_reglements_cond(self, args): #Done
        query = '''
            SELECT 
                T_REGELEMENT_COND.ID_REGLEMENT AS ID_REGLEMENT,	
                T_REGELEMENT_COND.CODE_OPERTAEUR AS CODE_OPERTAEUR,	
                T_REGELEMENT_COND.MONTANT_REGLER AS MONTANT_REGLER,	
                T_REGELEMENT_COND.REGLER_C_STD AS REGLER_C_STD,	
                T_REGELEMENT_COND.REGLER_P_AG AS REGLER_P_AG,	
                T_REGELEMENT_COND.REGLER_P_UHT AS REGLER_P_UHT,	
                T_REGELEMENT_COND.REGLER_C_AG AS REGLER_C_AG,	
                T_REGELEMENT_COND.REGLER_C_PR AS REGLER_C_PR,	
                T_REGELEMENT_COND.Date_Creation AS Date_Creation,	
                T_REGELEMENT_COND.DATE_VALIDATION AS DATE_VALIDATION,	
                T_REGELEMENT_COND.REGLER_C_BLC AS REGLER_C_BLC,	
                T_REGELEMENT_COND.REGLER_P_EURO AS REGLER_P_EURO,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_REGELEMENT_COND.REGLER_CS1 AS REGLER_CS1,	
                T_REGELEMENT_COND.REGLER_CS2 AS REGLER_CS2
            FROM 
                T_OPERATEUR,	
                T_REGELEMENT_COND
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_REGELEMENT_COND.CODE_OPERTAEUR
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_REGELEMENT_COND.DATE_VALIDATION = '{Param_date_validation}'
                )
            ORDER BY 
                ID_REGLEMENT DESC
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_REGELEMENT_COND.CODE_OPERTAEUR = {Param_code_operateur} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_operateur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_remise_clients(self, args):
        query = '''
            SELECT 
                T_REMISE_CLIENT.Date_Debut AS Date_Debut,	
                T_REMISE_CLIENT.Date_Fin AS Date_Fin,	
                T_REMISE_CLIENT.CODE_CLIENT AS CODE_CLIENT,	
                T_REMISE_CLIENT.DATE_REMISE AS DATE_REMISE,	
                T_REMISE_CLIENT.MT_REMISE AS MT_REMISE,	
                T_REMISE_CLIENT.MT_REPARTI AS MT_REPARTI,	
                T_REMISE_CLIENT.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_REMISE_CLIENT.MT_CA AS MT_CA,	
                T_REMISE_CLIENT.VALID AS VALID,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_REMISE_CLIENT.TX_DERIVES AS TX_DERIVES,	
                T_REMISE_CLIENT.CA_MOY AS CA_MOY,	
                T_ZONE.CODE_SUPERVISEUR AS CODE_SUPERVISEUR,	
                T_ZONE.RESP_VENTE AS RESP_VENTE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_SUPERVISEUR,	
                T_RESP_VENTE.NOM_OPERATEUR AS NOM_ANIMATEUR,	
                T_CLIENTS.TYPE_PRESENTOIRE AS TYPE_PRESENTOIRE,	
                T_REMISE_CLIENT.MT_REMISE_LAIT AS MT_REMISE_LAIT,	
                T_REMISE_CLIENT.TX_LAIT AS TX_LAIT,	
                T_REMISE_CLIENT.STATUT AS STATUT
            FROM 
                T_REMISE_CLIENT,	
                T_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_OPERATEUR,	
                T_SECTEUR,	
                T_BLOC,	
                T_ZONE,	
                T_OPERATEUR T_RESP_VENTE
            WHERE 
                T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_RESP_VENTE.CODE_OPERATEUR = T_ZONE.RESP_VENTE
                AND		T_OPERATEUR.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_SOUS_SECTEUR.code_secteur = T_SECTEUR.code_secteur
                AND		T_REMISE_CLIENT.CODE_CLIENT = T_CLIENTS.CODE_CLIENT
                AND
                (
                    T_REMISE_CLIENT.Date_Debut BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_REMISE_CLIENT.CODE_CLIENT = {Param_code_client}
                    AND	T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_REMISE_CLIENT.STATUT >= 0
                )
            ORDER BY 
                NOM_ANIMATEUR ASC,	
                NOM_SUPERVISEUR ASC,	
                NOM_SECTEUR ASC,	
                NOM_CLIENT ASC
        '''
        return query.format(**kwargs)

    
    def Req_ls_remises_clients(self, args):
        query = '''
            SELECT 
                T_REMISE_CLIENT.Date_Debut AS Date_Debut,	
                T_REMISE_CLIENT.Date_Fin AS Date_Fin,	
                T_REMISE_CLIENT.CODE_CLIENT AS CODE_CLIENT,	
                T_REMISE_CLIENT.DATE_REMISE AS DATE_REMISE,	
                T_REMISE_CLIENT.MT_REMISE AS MT_REMISE,	
                T_REMISE_CLIENT.MT_REPARTI AS MT_REPARTI,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_ZONE.CODE_SUPERVISEUR AS CODE_SUPERVISEUR,	
                T_ZONE.RESP_VENTE AS RESP_VENTE,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR
            FROM 
                T_CLIENTS,	
                T_REMISE_CLIENT,	
                T_SOUS_SECTEUR,	
                T_SECTEUR,	
                T_BLOC,	
                T_ZONE
            WHERE 
                T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLIENTS.CODE_CLIENT = T_REMISE_CLIENT.CODE_CLIENT
                AND
                (
                    T_REMISE_CLIENT.Date_Debut = {Param_date_debut}
                    AND	T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_ZONE.RESP_VENTE = {Param_code_resp_vente}
                )
            ORDER BY 
                NOM_SECTEUR ASC,	
                NOM_CLIENT ASC
        '''
        return query.format(**kwargs)

    
    def Req_ls_remises_par_secteur(self, args): #Done
        query = '''
            SELECT 
                T_REMISE_CLIENT.Date_Debut AS Date_Debut,	
                T_REMISE_CLIENT.Date_Fin AS Date_Fin,	
                T_REMISE_CLIENT.CODE_CLIENT AS CODE_CLIENT,	
                T_REMISE_CLIENT.MT_REMISE AS MT_REMISE,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_ZONE.CODE_SUPERVISEUR AS CODE_SUPERVISEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT
            FROM 
                T_REMISE_CLIENT,	
                T_ZONE,	
                T_BLOC,	
                T_SECTEUR,	
                T_SOUS_SECTEUR,	
                T_OPERATEUR,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_REMISE_CLIENT.CODE_CLIENT
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_OPERATEUR.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_REMISE_CLIENT.Date_Debut = '{Param_dt1}'
                    AND	T_REMISE_CLIENT.Date_Fin = '{Param_dt2}'
                )
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_ls_resp_vente(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.ACTIF AS ACTIF
            FROM 
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.ACTIF = 1
                AND	T_OPERATEUR.FONCTION = 15
            ORDER BY 
                NOM_OPERATEUR ASC
        '''
        return query

    
    def Req_ls_secteur(self, args): #Done
        query = '''
            SELECT 
                T_SECTEUR.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SECTEUR.RANG AS RANG,	
                T_SECTEUR.ACTIF AS ACTIF,	
                T_ZONE.NOM_ZONE AS NOM_ZONE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_SECTEUR.CAT_SECTEUR AS CAT_SECTEUR,	
                T_ZONE.CODE_SUPERVISEUR AS CODE_SUPERVISEUR,	
                T_SECTEUR.PREVENTE AS PREVENTE
            FROM 
                T_ZONE,	
                T_BLOC,	
                T_SECTEUR,	
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR[1]
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND
                (
                    T_SECTEUR.ACTIF = 1
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_code_superviseur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_superviseur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_secteur2(self, args): #Done
        query = '''
            SELECT 
                T_SECTEUR.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SECTEUR.RANG AS RANG,	
                T_SECTEUR.ACTIF AS ACTIF,	
                T_ZONE.NOM_ZONE AS NOM_ZONE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_SECTEUR.CAT_SECTEUR AS CAT_SECTEUR
            FROM 
                T_ZONE,	
                T_BLOC,	
                T_SECTEUR,	
                T_OPERATEUR
            WHERE 
                T_ZONE.CODE_SUPERVISEUR = T_OPERATEUR.CODE_OPERATEUR
                AND		T_SECTEUR.CODE_BLOC = T_BLOC.CODE_BLOC
                AND		T_BLOC.CODE_ZONE = T_ZONE.CODE_ZONE
                AND
                (
                    T_SECTEUR.ACTIF = 1
                )
            ORDER BY 
                RANG ASC
        '''
        return query

    
    def Req_ls_secteur_prevente(self, args): #Done
        query = '''
            SELECT 
                T_SECTEUR.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SECTEUR.RANG AS RANG,	
                T_SECTEUR.ACTIF AS ACTIF,	
                T_ZONE.NOM_ZONE AS NOM_ZONE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_SECTEUR.CAT_SECTEUR AS CAT_SECTEUR,	
                T_ZONE.CODE_SUPERVISEUR AS CODE_SUPERVISEUR,	
                T_SECTEUR.PREVENTE AS PREVENTE
            FROM 
                T_ZONE,	
                T_BLOC,	
                T_SECTEUR,	
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR[1]
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND
                (
                    T_SECTEUR.ACTIF = 1
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_SECTEUR.PREVENTE = 1
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_code_superviseur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_superviseur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_secteur_sans_commande(self, args): #Done
        query = '''
            SELECT 
                T_SECTEUR.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SECTEUR.RANG AS RANG,	
                T_SECTEUR.ACTIF AS ACTIF
            FROM 
                T_SECTEUR
            WHERE 
                T_SECTEUR.ACTIF = 1
                AND	T_SECTEUR.code_secteur NOT IN ({Param_cds}) 
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_cds': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_cds'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_ss_secteur(self, args): #Done
        query = '''
            SELECT 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR AS CODE_SOUS_SECTEUR,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_SOUS_SECTEUR.RANG AS RANG
            FROM 
                T_SOUS_SECTEUR
            WHERE 
                T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_ss_tournee(self, args): #Done
        query = '''
            SELECT 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR AS CODE_SOUS_SECTEUR,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur
            FROM 
                T_SOUS_SECTEUR
            WHERE 
                T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_superviseurs(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.ACTIF AS ACTIF
            FROM 
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.ACTIF = 1
                AND	T_OPERATEUR.FONCTION = 8
            ORDER BY 
                NOM_OPERATEUR ASC
        '''
        return query

    
    def Req_ls_superviseurs_resp_vente(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.ACTIF AS ACTIF,	
                T_ZONE.RESP_VENTE AS RESP_VENTE
            FROM 
                T_ZONE,	
                T_OPERATEUR
            WHERE 
                T_ZONE.CODE_SUPERVISEUR = T_OPERATEUR.CODE_OPERATEUR
                AND
                (
                    T_OPERATEUR.ACTIF = 1
                    AND	T_ZONE.RESP_VENTE = {param_resp_vente}
                )
            ORDER BY 
                NOM_OPERATEUR ASC
        '''

        try:
            kwargs = {
                'param_resp_vente': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['param_resp_vente'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_tache_operateur(self, args): #Done
        query = '''
            SELECT 
                T_OPERTEURS_TACHES.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERTEURS_TACHES.ID_TACHE AS ID_TACHE,	
                T_OPERTEURS_TACHES.ETAT AS ETAT,	
                T_OPERATEUR.Matricule AS Matricule
            FROM 
                T_OPERTEURS_TACHES,	
                T_OPERATEUR
            WHERE 
                T_OPERTEURS_TACHES.CODE_OPERATEUR = T_OPERATEUR.CODE_OPERATEUR
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0]
            }
        except IndexError as e:
            return e

        kwargs['OPTIONAL_ARG_1'] = '''AND
                (
                    T_OPERTEURS_TACHES.CODE_OPERATEUR = {Param_code_operateur}
                )'''
        
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_operateur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_ls_tournees(self, args): #Done
        query = '''
            SELECT 
                T_TOURNEES.CODE_TOURNEE AS CODE_TOURNEE,	
                T_TOURNEES.NOM_TOURNEE AS NOM_TOURNEE,	
                T_TOURNEES.code_secteur AS code_secteur,	
                T_TOURNEES.ROTATION AS ROTATION,	
                T_TOURNEES.NBRE_JOURS AS NBRE_JOURS,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR
            FROM 
                T_SECTEUR,	
                T_TOURNEES
            WHERE 
                T_SECTEUR.code_secteur = T_TOURNEES.code_secteur
                AND
                (
                    T_TOURNEES.code_secteur = {Param_code_secteur}
                )
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_tous_secteur(self, args): #Done
        query = '''
            SELECT 
                T_SECTEUR.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SECTEUR.RANG AS RANG,	
                T_SECTEUR.ACTIF AS ACTIF,	
                T_ZONE.NOM_ZONE AS NOM_ZONE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_SECTEUR.CAT_SECTEUR AS CAT_SECTEUR,	
                T_ZONE.CODE_SUPERVISEUR AS CODE_SUPERVISEUR
            FROM 
                T_ZONE,	
                T_BLOC,	
                T_SECTEUR,	
                T_OPERATEUR
            WHERE 
                T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_OPERATEUR.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR[1]
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND
                (
                    T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                )
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_code_superviseur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_superviseur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_vehicule(self, args): #Done
        query = '''
            SELECT 
                T_VEHICULE.Matricule AS Matricule,	
                T_VEHICULE.ACTIF AS ACTIF,	
                T_VEHICULE.Id_Vehicule AS Id_Vehicule,	
                T_VEHICULE.Id_Type_Vehicule AS Id_Type_Vehicule
            FROM 
                T_VEHICULE
            WHERE 
                T_VEHICULE.ACTIF = 1
                AND	T_VEHICULE.Id_Type_Vehicule = {ParamId_Type_Vehicule}
        '''

        try:
            kwargs = {
                'ParamId_Type_Vehicule': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['ParamId_Type_Vehicule'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ls_vendeurs(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.ACTIF AS ACTIF,	
                T_OPERATEUR.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_OPERATEUR.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_FONCTION.NOM_FONCTION AS NOM_FONCTION,	
                T_OPERATEUR.CODE_INTERNE AS CODE_INTERNE
            FROM 
                T_FONCTION,	
                T_OPERATEUR
            WHERE 
                T_FONCTION.CODE_FONCTION = T_OPERATEUR.FONCTION
                AND
                (
                    T_OPERATEUR.ACTIF = 1
                    AND	T_OPERATEUR.FONCTION IN (1, 4, 5, 6) 
                )
        '''
        return query

    
    def Req_ls_vendeurs_depositaires(self, args):
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.ACTIF AS ACTIF
            FROM 
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.ACTIF = 1
                AND	T_OPERATEUR.FONCTION IN (1, 4, 5, 6, 11, 55, 56) 
        '''
        return query

    
    def Req_ls_versement_caisse(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS_CAISSE.NUM_PIECE AS NUM_PIECE,	
                T_OPERATIONS_CAISSE.COMPTE_VERSEMENT AS COMPTE_VERSEMENT,	
                T_OPERATIONS_CAISSE.COMMENTAIRE AS COMMENTAIRE,	
                T_OPERATIONS_CAISSE.MONTANT AS MONTANT,	
                T_COMPTES.NUM_COMPTE AS NUM_COMPTE,	
                T_BANQUES.LIBELLE AS LIBELLE,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION
            FROM 
                T_BANQUES,	
                T_COMPTES,	
                T_OPERATIONS_CAISSE
            WHERE 
                T_BANQUES.NUM_BANQUE = T_COMPTES.BANQUE
                AND		T_COMPTES.CODE_COMPTE = T_OPERATIONS_CAISSE.COMPTE_VERSEMENT
                AND
                (
                    T_OPERATIONS_CAISSE.DATE_VALIDATION = '{Param_date_versement}'
                    AND	T_OPERATIONS_CAISSE.TYPE_OPERATION = {Param_type_operation}
                )
            ORDER BY 
                CODE_OPERATION DESC
        '''

        try:
            kwargs = {
                'Param_date_versement': args[0],
                'Param_type_operation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_versement'] = self.validateDate(kwargs['Param_date_versement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_magasin_article(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES_MAGASINS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES_MAGASINS.MAGASIN AS MAGASIN,	
                SUM(T_ARTICLES_MAGASINS.QTE_STOCK) AS la_somme_QTE_STOCK
            FROM 
                T_ARTICLES_MAGASINS
            WHERE 
                T_ARTICLES_MAGASINS.CODE_ARTICLE = {Param_code_article}
                {OPTIONAL_ARG_1}
            GROUP BY 
                T_ARTICLES_MAGASINS.CODE_ARTICLE,	
                T_ARTICLES_MAGASINS.MAGASIN
        '''

        try:
            kwargs = {
                'Param_code_article': args[0],
                'Param_code_magasin': args[1]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_code_article'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_ARTICLES_MAGASINS.MAGASIN = {Param_code_magasin}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_magasin'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_magasin_categorie(self, args): #Done
        query = '''
            SELECT 
                T_TYPE_PRODUIT_MAG.CODE_MAGASIN AS CODE_MAGASIN,	
                T_TYPE_PRODUIT_MAG.PRODUIT AS PRODUIT,	
                T_MAGASINS.NOM_MAGASIN AS NOM_MAGASIN
            FROM 
                T_MAGASINS,	
                T_TYPE_PRODUIT_MAG
            WHERE 
                T_MAGASINS.CODE_MAGASIN = T_TYPE_PRODUIT_MAG.CODE_MAGASIN
                AND
                (
                    T_TYPE_PRODUIT_MAG.PRODUIT = {Param_categorie}
                )
        '''

        try:
            kwargs = {
                'Param_categorie': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_categorie'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_magasin_cond(self, args): #Done
        query = '''
            SELECT 
                T_MAGASIN_COND.CODE_MAGASIN AS CODE_MAGASIN,	
                T_MAGASIN_COND.CODE_CP AS CODE_CP,	
                T_MAGASIN_COND.QTE_STOCK AS QTE_STOCK,	
                T_MAGASINS.NOM_MAGASIN AS NOM_MAGASIN
            FROM 
                T_MAGASINS,	
                T_MAGASIN_COND
            WHERE 
                T_MAGASINS.CODE_MAGASIN = T_MAGASIN_COND.CODE_MAGASIN
                AND
                (
                    T_MAGASIN_COND.CODE_CP = {Param_code_cp}
                )
        '''

        try:
            kwargs = {
                'Param_code_cp': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_cp'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_magasins_agence(self, args): #Done
        query = '''
            SELECT 
                T_MAGASINS.CODE_MAGASIN AS CODE_MAGASIN,	
                T_MAGASINS.NOM_MAGASIN AS NOM_MAGASIN,	
                T_MAGASINS.CODE_AGCE AS CODE_AGCE
            FROM 
                T_MAGASINS
            WHERE 
                T_MAGASINS.CODE_AGCE = {Param_code_agce}
        '''

        try:
            kwargs = {
                'Param_code_agce': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_agce'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_maj_pass(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_OPERATEUR.MDP AS MDP,	
                T_OPERATEUR.ACTIF AS ACTIF
            FROM 
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.FONCTION IN (7) 
                AND	T_OPERATEUR.ACTIF = 1
        '''
        return query

    
    def Req_maj_position_cond(self, args): #Done
        query = '''
            UPDATE 
                T_MAGASIN_COND
            SET
                QTE_STOCK = {Param_qte_stock}
            WHERE 
                {OPTIONAL_ARG_1}
                T_MAGASIN_COND.CODE_CP = {Param_code_cp}
        '''

        try:
            kwargs = {
                'Param_qte_stock': args[0],
                'Param_code_magasin': args[1],
                'Param_code_cp': args[2]
            }
        except IndexError as e:
            return e
        
        for key in ('Param_qte_stock', 'Param_code_cp'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_MAGASIN_COND.CODE_MAGASIN = {Param_code_magasin} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_magasin'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_mappage_article(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.ACTIF AS ACTIF,	
                T_MAPPAGE_ARTICLE.CODE_INTERNE AS CODE_INTERNE,	
                T_ARTICLES.RANG AS RANG
            FROM 
                T_ARTICLES
                LEFT OUTER JOIN
                T_MAPPAGE_ARTICLE
                ON T_ARTICLES.CODE_ARTICLE = T_MAPPAGE_ARTICLE.CODE_ARTICLE
            WHERE 
                (
                T_ARTICLES.ACTIF = 1
            )
            ORDER BY 
                RANG ASC
        '''
        return query

    
    def Req_mappage_produit(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                T_MAPPAGE_PRODUIT.CODE_INTERNE AS CODE_INTERNE
            FROM 
                T_PRODUITS
                LEFT OUTER JOIN
                T_MAPPAGE_PRODUIT
                ON T_PRODUITS.CODE_PRODUIT = T_MAPPAGE_PRODUIT.CODE_PRODUIT
        '''
        return query

    
    def Req_mappage_secteur(self, args): #Done
        query = '''
            SELECT 
                T_SECTEUR.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_MAPPAGE_SECTEUR.CODE_INTERNE AS CODE_INTERNE,	
                T_SECTEUR.RANG AS RANG
            FROM 
                T_SECTEUR
                LEFT OUTER JOIN
                T_MAPPAGE_SECTEUR
                ON T_SECTEUR.code_secteur = T_MAPPAGE_SECTEUR.code_secteur
            ORDER BY 
                RANG ASC
        '''
        return query

    
    def Req_max_autorisation(self, args): #Done
        query = '''
            SELECT 
                MAX(T_AUTORISATIONS_SOLDE.ID_JUSTIFICATION) AS le_maximum_ID_JUSTIFICATION
            FROM 
                T_AUTORISATIONS_SOLDE
        '''
        return query

    
    def Req_max_autorisation_caisserie(self, args): #Done
        query = '''
            SELECT 
                MAX(T_AUTORISATION_SOLDE_CAISSERIE.ID_JUSTIFICATION) AS le_maximum_ID_JUSTIFICATION
            FROM 
                T_AUTORISATION_SOLDE_CAISSERIE
        '''
        return query

    
    def Req_max_borderau_valeurs(self, args): #Done
        query = '''
            SELECT 
                MAX(T_BORDEREAU_VALEURS.ID_BORDEREAU) AS le_maximum_CODE_OPERATION
            FROM 
                T_BORDEREAU_VALEURS
        '''
        return query

    
    def Req_max_chargement(self, args): #Done
        query = '''
            SELECT 
                MAX(T_CHARGEMENT.CODE_CHARGEMENT) AS le_maximum_CODE_CHARGEMENT
            FROM 
                T_CHARGEMENT
        '''
        return query

    
    def Req_max_commandes_usine(self, args): #Done
        query = '''
            SELECT 
                MAX(T_COMMANDES.ID_COMMANDE) AS le_maximum_ID_COMMANDE
            FROM 
                T_COMMANDES
        '''
        return query

    
    def Req_max_cond_livrees(self, args): #Done
        query = '''
            SELECT 
                MAX(T_COND_LIVRAISON.NUM_LIVRAISON) AS le_maximum_NUM_LIVRAISON
            FROM 
                T_COND_LIVRAISON
        '''
        return query

    
    def Req_max_convoyage(self, args): #Done
        query = '''
            SELECT 
                MAX(T_CONVOYAGE.NUM_CONVOYAGE) AS max_convoyage
            FROM 
                T_CONVOYAGE
        '''
        return query

    
    def Req_max_decompte(self, args): #Done
        query = '''
            SELECT 
                MAX(T_DECOMPTE.NUM_DECOMPTE) AS le_maximum_NUM_DECOMPTE
            FROM 
                T_DECOMPTE
        '''
        return query

    
    def Req_max_envoi(self, args): #Done
        query = '''
            SELECT 
                MAX(T_COURRIER_AGENCE.ID_ENVOI) AS le_maximum_ID
            FROM 
                T_COURRIER_AGENCE
        '''
        return query

    
    def Req_max_journee(self, args): #Done
        query = '''
            SELECT 
                MAX(T_JOURNEE.DATE_JOURNEE) AS le_maximum_DATE_JOURNEE,	
                T_JOURNEE.CLOTURE AS CLOTURE
            FROM 
                T_JOURNEE
            GROUP BY 
                T_JOURNEE.CLOTURE
            ORDER BY 
                le_maximum_DATE_JOURNEE DESC
        '''
        return query

    
    def Req_max_livraison(self, args): #Done
        query = '''
            SELECT 
                MAX(T_LIVRAISON.NUM_LIVRAISON) AS le_maximum_NUM_LIVRAISON
            FROM 
                T_LIVRAISON
        '''
        return query

    
    def Req_max_mouv_cond(self, args): #Done
        query = '''
            SELECT 
                MAX(T_MOUVEMENTS_CAISSERIE.ID_MOUVEMENT) AS le_maximum_ID_MOUVEMENT
            FROM 
                T_MOUVEMENTS_CAISSERIE
        '''
        return query

    
    def Req_max_mouvement(self, args): #Done
        query = '''
            SELECT 
                MAX(T_MOUVEMENTS.ID_MOUVEMENT) AS le_maximum_ID_MOUVEMENT
            FROM 
                T_MOUVEMENTS
        '''
        return query

    
    def Req_max_mvt_caisse(self, args): #Done
        query = '''
            SELECT 
                MAX(T_MOUVEMENTS_CAISSE.ID_MOUVEMENT) AS le_maximum_ID_MOUVEMENT
            FROM 
                T_MOUVEMENTS_CAISSE
        '''
        return query

    
    def Req_max_operations_caisse(self, args): #Done
        query = '''
            SELECT 
                MAX(T_OPERATIONS_CAISSE.CODE_OPERATION) AS le_maximum_CODE_OPERATION
            FROM 
                T_OPERATIONS_CAISSE
        '''
        return query

    
    def Req_max_prelevement(self, args): #Done
        query = '''
            SELECT 
                MAX(T_PRELEVEMENT_SUSP_COND.ID_PRELEV) AS le_maximum_ID_PRELEV
            FROM 
                T_PRELEVEMENT_SUSP_COND
        '''
        return query

    
    def Req_max_reclamation(self, args): #Done
        query = '''
            SELECT 
                MAX(T_RECLAMATIONS_QUALITE.ID_RECLAMATION) AS le_maximum_ID_RECLAMATION
            FROM 
                T_RECLAMATIONS_QUALITE
        '''
        return query

    
    def Req_max_reconaissance(self, args): #Done
        query = '''
            SELECT 
                MAX(T_RECONAISSANCES.ID_RECONAISSANCE) AS le_maximum_ID_RECONAISSANCE
            FROM 
                T_RECONAISSANCES
        '''
        return query

    
    def Req_max_reglement(self, args): #Done
        query = '''
            SELECT 
                MAX(T_REGELEMENT_COND.ID_REGLEMENT) AS le_maximum_ID_REGLEMENT
            FROM 
                T_REGELEMENT_COND
        '''
        return query

    
    def Req_max_tournee(self, args): #Done
        query = '''
            SELECT 
                MAX(T_TOURNEES.CODE_TOURNEE) AS le_maximum_CODE_TOURNEE
            FROM 
                T_TOURNEES
        '''
        return query

    
    def Req_min_rang_produit(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                MIN(T_ARTICLES.RANG) AS le_minimum_RANG
            FROM 
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_PRODUIT = {Param_code_produit}
            GROUP BY 
                T_ARTICLES.CODE_PRODUIT
        '''
        
        try:
            kwargs = {
                'Param_code_produit': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_produit'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_modele_taches(self, args): #Done
        query = '''
            SELECT 
                T_MODELE_TACHES.ID_MODELE AS ID_MODELE,	
                T_MODELE_TACHES.ID_TACHE AS ID_TACHE,	
                T_MODELE_TACHES.ETAT AS ETAT
            FROM 
                T_MODELE_TACHES
            WHERE 
                T_MODELE_TACHES.ID_MODELE = {Param_id_modele}
        '''
                
        try:
            kwargs = {
                'Param_id_modele': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_id_modele'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_montant_a_verser(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_CHARGEMENT.VALID AS VALID,	
                SUM(T_CHARGEMENT.MONTANT_A_VERSER) AS la_somme_MONTANT_A_VERSER,	
                T_OPERATEUR.FONCTION AS FONCTION
            FROM 
                T_OPERATEUR,	
                T_CHARGEMENT
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND
                (
                    T_CHARGEMENT.VALID = 1
                    AND	T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                    AND	T_OPERATEUR.FONCTION = {Param_fonction}
                )
            GROUP BY 
                T_CHARGEMENT.DATE_CHARGEMENT,	
                T_CHARGEMENT.code_vendeur,	
                T_CHARGEMENT.VALID,	
                T_OPERATEUR.FONCTION
        '''

        
        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_fonction': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])
        
        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_OPERATEUR.FONCTION = {Param_fonction}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_fonction'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_montant_livraison_client(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_LIVREES.CODE_CLIENT AS CODE_CLIENT,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_LIVREES.QTE_CHARGEE AS QTE_CHARGEE,	
                T_PRODUITS_LIVREES.MONTANT AS MONTANT,	
                T_PRODUITS_LIVREES.PRIX AS PRIX,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.STATUT AS STATUT,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.DATE_VALIDATION = '{Param_date_validation}'
                    AND	T_LIVRAISON.TYPE_MVT IN ('L', 'R') 
                )
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_mouvements_caisserie(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_COND_CHARGEE.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                T_COND_CHARGEE.QTE_POINTE AS QTE_POINTE,	
                T_COND_CHARGEE.QTE_CHAR_SUPP AS QTE_CHAR_SUPP,	
                T_COND_CHARGEE.QTE_RETOUR AS QTE_RETOUR,	
                T_SECTEUR.RANG AS RANG
            FROM 
                T_CHARGEMENT,	
                T_COND_CHARGEE,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_COND_CHARGEE.CODE_CHARGEMENT
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                )
            ORDER BY 
                RANG ASC
        '''
        
        try:
            kwargs = {
                'Param_date_chargement': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_moy_vente_gms(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_LIVREES.CODE_CLIENT AS CODE_CLIENT,	
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_PRODUITS_LIVREES.TYPE_CLIENT AS TYPE_CLIENT,	
                T_PRODUITS_LIVREES.TYPE_MVT AS TYPE_MVT,	
                AVG(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_moyenne_QTE_CHARGEE
            FROM 
                T_PRODUITS,	
                T_PRODUITS_LIVREES,	
                T_ARTICLES,	
                T_LIVRAISON
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRODUITS_LIVREES.CODE_ARTICLE[1]
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND
                (
                    T_PRODUITS_LIVREES.DATE_VALIDATION BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_PRODUITS_LIVREES.TYPE_CLIENT = 1
                    AND	T_PRODUITS_LIVREES.CODE_CLIENT = {Param_code_client}
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
            GROUP BY 
                T_PRODUITS.CODE_PRODUIT,	
                T_PRODUITS_LIVREES.TYPE_CLIENT,	
                T_PRODUITS_LIVREES.TYPE_MVT,	
                T_PRODUITS_LIVREES.CODE_CLIENT
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_code_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_moyenne_vente(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_VENDU + T_PRODUITS_CHARGEE.CREDIT ) ) AS la_total_VENTE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE) AS la_somme_TOTAL_INVENDU_POINTE
            FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
            GROUP BY 
                T_PRODUITS_CHARGEE.CODE_ARTICLE
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_moyenne_vente_produit_secteur(self, args):
        query = '''
            SELECT 
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_VENDU) AS la_somme_TOTAL_VENDU,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE) AS la_somme_TOTAL_INVENDU_POINTE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM) AS la_somme_TOTAL_RENDUS_COM,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_CHARGEE) AS la_somme_TOTAL_CHARGEE,	
                SUM(T_PRODUITS_CHARGEE.CREDIT) AS la_somme_CREDIT
            FROM 
                T_PRODUITS,	
                T_ARTICLES,	
                T_PRODUITS_CHARGEE,	
                T_CHARGEMENT,	
                T_OPERATEUR
            WHERE 
                T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_CHARGEE.CODE_ARTICLE
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND		T_OPERATEUR.CODE_OPERATEUR[1] = T_CHARGEMENT.code_vendeur
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}
                    AND	T_ARTICLES.CODE_PRODUIT = {Param_code_produit}
                    AND	T_PRODUITS.CODE_FAMILLE = {Param_code_famille}
                )
            GROUP BY 
                T_CHARGEMENT.code_secteur,	
                T_PRODUITS.CODE_PRODUIT,	
                T_OPERATEUR.FONCTION
        '''
        return query.format(**kwargs)

    
    def Req_mt_a_verser_secteur(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.MONTANT_A_VERSER AS MONTANT_A_VERSER
            FROM 
                T_CHARGEMENT
            WHERE 
                T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_mt_remise_secteur(self, args): #Done
        query = '''
            SELECT 
                T_DT_FACTURE.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(( ( ( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_REMISE ) * T_DT_FACTURE.PRIX ) * T_DT_FACTURE.TX_GRATUIT ) /  100) ) AS MT_REMISE,	
                SUM(( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) ) AS SOMME_QTE
            FROM 
                T_FACTURE,	
                T_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_DT_FACTURE
            WHERE 
                T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_DT_FACTURE.TX_GRATUIT <> 0
                    AND	T_SOUS_SECTEUR.code_secteur = {Param_CODE_SECTEUR}
                    AND	T_CLIENTS.CLIENT_EN_COMPTE = 0
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{param_dt1}' AND '{param_dt2}'
                )
            GROUP BY 
                T_DT_FACTURE.CODE_ARTICLE
        '''

        try:
            kwargs = {
                'Param_CODE_SECTEUR': args[0],
                'param_dt1': args[1],
                'param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['param_dt1'] = self.validateDate(kwargs['param_dt1'])
        kwargs['param_dt2'] = self.validateDate(kwargs['param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_mt_verse_operateur(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT,	
                T_DECOMPTE.DATE_DECOMPTE AS DATE_DECOMPTE,	
                SUM(T_DECOMPTE.MONTANT) AS la_somme_MONTANT
            FROM 
                T_DECOMPTE
            WHERE 
                T_DECOMPTE.MODE_PAIEMENT = 'E'
                {OPTIONAL_ARG_1}
                {OPTIONAL_ARG_2}
            GROUP BY 
                T_DECOMPTE.CODE_OPERATEUR,	
                T_DECOMPTE.MODE_PAIEMENT,	
                T_DECOMPTE.DATE_DECOMPTE
        '''

        try:
            kwargs = {
                'Param_date_decompte': args[0],
                'Param_code_operateur': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_decompte'] = self.validateDate(kwargs['Param_date_decompte'])

        kwargs['OPTIONAL_ARG_1'] = '''AND T_DECOMPTE.DATE_DECOMPTE = '{Param_date_decompte}' '''
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_DECOMPTE.CODE_OPERATEUR = {Param_code_operateur}'

        if kwargs['Param_date_decompte'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
            kwargs['OPTIONAL_ARG_2'] = kwargs['OPTIONAL_ARG_2'][4:]

        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_operateur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_mvente(self, args): #Done
        query = '''
            SELECT 
                T_MOYENNE_VENTE.DATE_VENTE AS DATE_VENTE,	
                T_MOYENNE_VENTE.code_secteur AS code_secteur,	
                T_MOYENNE_VENTE.CA_MOYENNE AS CA_MOYENNE
            FROM 
                T_MOYENNE_VENTE
            WHERE 
                T_MOYENNE_VENTE.DATE_VENTE = '{Param_date_vente}'
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_date_vente': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_vente'] = self.validateDate(kwargs['Param_date_vente'])

        if kwargs['Param_date_vente'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_MOYENNE_VENTE.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_nbl_client(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT
            FROM 
                T_LIVRAISON
            WHERE 
                T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                AND	T_LIVRAISON.CODE_CLIENT = {Param_code_client}
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_client': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_nbre_facture_client(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                COUNT(T_FACTURE.NUM_FACTURE) AS Comptage_1
            FROM 
                T_FACTURE
            WHERE 
                T_FACTURE.DATE_HEURE BETWEEN '{Param1}' AND '{Param2}'
                AND	T_FACTURE.CODE_CLIENT = {Param_code_client}
            GROUP BY 
                T_FACTURE.CODE_CLIENT
        '''

        try:
            kwargs = {
                'Param1': args[0],
                'Param2': args[1],
                'Param_code_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param1'] = self.validateDate(kwargs['Param1'])
        kwargs['Param2'] = self.validateDate(kwargs['Param2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_nouv_solde_dep(self, args): #Done
        query = '''
            SELECT 
                SUM(T_MOUVEMENTS_CAISSE.MONTANT) AS la_somme_MONTANT,	
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION
            FROM 
                T_OPERATIONS_CAISSE,	
                T_MOUVEMENTS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.CODE_OPERATION = T_MOUVEMENTS_CAISSE.ORIGINE
                AND
                (
                    T_MOUVEMENTS_CAISSE.CODE_CAISSE = {Param_code_caisse}
                    AND	T_OPERATIONS_CAISSE.DATE_OPERATION = '{Param_date_journee}'
                    AND	T_OPERATIONS_CAISSE.DATE_VALIDATION = '1900-01-01 00:00:00'
                    AND	T_OPERATIONS_CAISSE.TYPE_OPERATION IN ('D', 'V') 
                )
            GROUP BY 
                T_OPERATIONS_CAISSE.DATE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION
        '''

        try:
            kwargs = {
                'Param_code_caisse': args[0],
                'Param_date_journee': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_objectif_agence(self, args): #Done
        query = '''
            SELECT 
                T_OBJECTIF_AGENCE.DATE_OBJECTIF AS DATE_OBJECTIF,	
                T_OBJECTIF_AGENCE.CODE_PRODUIT AS CODE_PRODUIT,	
                T_OBJECTIF_AGENCE.OBJECTIF_TRAD AS OBJECTIF_TRAD,	
                T_OBJECTIF_AGENCE.OBJECTIF_GMS AS OBJECTIF_GMS,	
                T_OBJECTIF_AGENCE.OBJECTIF AS OBJECTIF
            FROM 
                T_OBJECTIF_AGENCE
            WHERE 
                T_OBJECTIF_AGENCE.DATE_OBJECTIF = '{Param_dt_objectif}'
        '''
        
        try:
            kwargs = {
                'Param_dt_objectif': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt_objectif'] = self.validateDate(kwargs['Param_dt_objectif'])

        if kwargs['Param_dt_objectif'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_objectif_clients(self, args): #Done
        query = '''
            SELECT 
                T_OBJECTIF_CLIENTS.DATE_OBJECTIF AS DATE_OBJECTIF,	
                T_OBJECTIF_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_OBJECTIF_CLIENTS.QTE_OBJECTIF AS QTE_OBJECTIF,	
                T_OBJECTIF_CLIENTS.CODE_PRODUIT AS CODE_ARTICLE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PROD
            FROM 
                T_OBJECTIF_CLIENTS,	
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_OBJECTIF_CLIENTS.CODE_PRODUIT
                AND
                (
                    T_OBJECTIF_CLIENTS.DATE_OBJECTIF = '{Param_date_objectif}'
                    AND	T_OBJECTIF_CLIENTS.CODE_CLIENT = {Param_code_client}
                )
        '''

        try:
            kwargs = {
                'Param_date_objectif': args[0],
                'Param_code_client': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_objectif'] = self.validateDate(kwargs['Param_date_objectif'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_objectif_mois(self, args): #Done
        query = '''
            SELECT 
                T_OBJECTIF_VENTE.DATE_OBJECTIF AS DATE_OBJECTIF,	
                T_OBJECTIF_VENTE.code_secteur AS code_secteur,	
                T_OBJECTIF_VENTE.MONTANT_OBJECTIF AS MONTANT_OBJECTIF
            FROM 
                T_OBJECTIF_VENTE
            WHERE 
                T_OBJECTIF_VENTE.DATE_OBJECTIF = '{Param_date_objectif}'
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_date_objectif': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_objectif'] = self.validateDate(kwargs['Param_date_objectif'])

        if kwargs['Param_date_objectif'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_OBJECTIF_VENTE.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_objectif_perte(self, args): #Done
        query = '''
            SELECT 
                T_OBJECTIF_RENDUS.DATE_OBJECTIF AS DATE_OBJECTIF,	
                T_OBJECTIF_RENDUS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_OBJECTIF_RENDUS.OBJ_RENDUS_US AS OBJ_RENDUS_US,	
                T_OBJECTIF_RENDUS.OBJ_RENDUS_AG AS OBJ_RENDUS_AG,	
                T_OBJECTIF_RENDUS.OBJ_RENDUS_SP AS OBJ_RENDUS_SP
            FROM 
                T_OBJECTIF_RENDUS
            WHERE 
                T_OBJECTIF_RENDUS.DATE_OBJECTIF = '{Param_date_objectif}'
        '''
        
        try:
            kwargs = {
                'Param_date_objectif': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_objectif'] = self.validateDate(kwargs['Param_date_objectif'])

        if kwargs['Param_date_objectif'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_objectif_secteurs(self, args): #Done
        query = '''
            SELECT 
                T_OBJECTIF_SECTEURS.DATE_OBJECTIF AS DATE_OBJECTIF,	
                T_OBJECTIF_SECTEURS.code_secteur AS code_secteur,	
                T_OBJECTIF_SECTEURS.QTE_OBJECTIF AS QTE_OBJECTIF,	
                T_OBJECTIF_SECTEURS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_OBJECTIF_SECTEURS.OBJECTIF_PERTE AS OBJECTIF_PERTE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PROD
            FROM 
                T_OBJECTIF_SECTEURS,	
                T_ARTICLES
            WHERE 
                T_OBJECTIF_SECTEURS.CODE_PRODUIT = T_ARTICLES.CODE_ARTICLE
                AND
                (
                    T_OBJECTIF_SECTEURS.DATE_OBJECTIF = '{Param_date_objectif}'
                    AND	T_OBJECTIF_SECTEURS.code_secteur = {Param_code_secteur}
                )
        '''
        
        try:
            kwargs = {
                'Param_date_objectif': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_objectif'] = self.validateDate(kwargs['Param_date_objectif'])

        if kwargs['Param_date_objectif'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_OBJECTIF_SECTEURS.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_objectifs_clients(self, args): #Done
        query = '''
            SELECT 
                T_BLOC.CODE_ZONE AS CODE_ZONE,	
                T_SOUS_SECTEUR.code_secteur AS code_secteur,	
                T_OBJECTIFS.CODE_CLIENT AS CODE_CLIENT,	
                T_OBJECTIFS.OBJECTIF AS OBJECTIF,	
                T_OBJECTIFS.REMISE_OBJECTIF AS REMISE_OBJECTIF
            FROM 
                T_OBJECTIFS,	
                T_BLOC,	
                T_SECTEUR,	
                T_SOUS_SECTEUR,	
                T_CLIENTS,	
                T_ZONE
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_OBJECTIFS.CODE_CLIENT
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_BLOC.CODE_ZONE = T_ZONE.CODE_ZONE
                AND		T_BLOC.CODE_BLOC = T_SECTEUR.CODE_BLOC
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND
                (
                    T_OBJECTIFS.OBJECTIF > 0
                    AND	T_OBJECTIFS.CODE_CLIENT = {Param_code_client}
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}
                )
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_code_superviseur': args[1],
                'Param_code_secteur': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_OBJECTIFS.CODE_CLIENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}'
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_superviseur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = 'AND	T_SOUS_SECTEUR.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_position_cond(self, args): #Done
        query = '''
            SELECT 
                T_MAGASIN_COND.CODE_MAGASIN AS CODE_MAGASIN,	
                T_MAGASIN_COND.CODE_CP AS CODE_CP,	
                T_MAGASIN_COND.QTE_STOCK AS QTE_STOCK
            FROM 
                T_MAGASIN_COND
            WHERE 
                T_MAGASIN_COND.CODE_MAGASIN = {Param_code_magasin}
                AND	T_MAGASIN_COND.CODE_CP = {Param_code_cp}
        '''

        try:
            kwargs = {
                'Param_code_magasin': args[0],
                'Param_code_cp': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_position_stock(self, args): #Done
        query = '''
            SELECT
                T_ARTICLES_MAGASINS.PK_T_ARTICLES_MAGASINS AS PK_T_ARTICLES_MAGASINS,
                T_ARTICLES_MAGASINS.MAGASIN AS MAGASIN,	
                T_ARTICLES_MAGASINS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES_MAGASINS.CATEGORIE AS CATEGORIE,	
                T_ARTICLES_MAGASINS.QTE_STOCK AS QTE_STOCK
            FROM 
                T_ARTICLES_MAGASINS
            WHERE 
                T_ARTICLES_MAGASINS.CODE_ARTICLE = {Param_code_article}
                AND	T_ARTICLES_MAGASINS.MAGASIN = {Param_code_magasin}
                AND	T_ARTICLES_MAGASINS.CATEGORIE = {Param_type_produit}
        '''

        try:
            kwargs = {
                'Param_code_article': args[0],
                'Param_code_magasin': args[1],
                'Param_type_produit': args[2]
            }
        except IndexError as e:
            return e

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_previsions(self, args): #Done
        query = '''
            SELECT 
                T_PREVISION.Date_Debut AS Date_Debut,	
                T_PREVISION.Date_Fin AS Date_Fin,	
                T_PREVISION.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PREVISION.QTE AS QTE
            FROM 
                T_PREVISION
            WHERE 
                T_PREVISION.Date_Debut = '{Param_date_debut}'
        '''
        
        try:
            kwargs = {
                'Param_date_debut': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_debut'] = self.validateDate(kwargs['Param_date_debut'])

        if kwargs['Param_date_debut'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_prix_article_periode(self, args): #Done
        query = '''
            SELECT 
                T_PRIX.Date_Debut AS Date_Debut,	
                T_PRIX.Date_Fin AS Date_Fin,	
                T_PRIX.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRIX.PRIX AS PRIX
            FROM 
                T_PRIX
            WHERE 
                T_PRIX.Date_Debut <= '{Param_dt1}'
                AND	T_PRIX.Date_Fin >= '{Param_dt1}'
                AND	T_PRIX.CODE_ARTICLE = {Param_code_article}
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_code_article': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_prix_debut_jour(self, args): #Done
        query = '''
            SELECT 
                T_PRIX.Date_Debut AS Date_Debut,	
                T_PRIX.CODE_AGCE AS CODE_AGCE,	
                T_PRIX.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRIX.PRIX AS PRIX
            FROM 
                T_PRIX
            WHERE 
                T_PRIX.Date_Debut = '{Param_date_debut}'
                AND	T_PRIX.CODE_AGCE = {Param_code_agce}
        '''

        try:
            kwargs = {
                'Param_date_debut': args[0],
                'Param_code_agce': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_debut'] = self.validateDate(kwargs['Param_date_debut'])

        if kwargs['Param_date_debut'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_produit_en_stock(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES_MAGASINS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES_MAGASINS.CATEGORIE AS CATEGORIE,	
                SUM(T_ARTICLES_MAGASINS.QTE_STOCK) AS la_somme_QTE_STOCK
            FROM 
                T_ARTICLES_MAGASINS
            WHERE 
                T_ARTICLES_MAGASINS.CATEGORIE = 'PRODUIT'
                AND	T_ARTICLES_MAGASINS.CODE_ARTICLE = {Param_code_article}
            GROUP BY 
                T_ARTICLES_MAGASINS.CODE_ARTICLE,	
                T_ARTICLES_MAGASINS.CATEGORIE
        '''

        try:
            kwargs = {
                'Param_code_article': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_article'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_produits_famille(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_PRODUITS.CODE_FAMILLE AS CODE_FAMILLE,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT
            FROM 
                T_PRODUITS
            WHERE 
                T_PRODUITS.CODE_FAMILLE = {Param_code_famille}
        '''

        try:
            kwargs = {
                'Param_code_famille': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_famille'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_produits_mappage(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                MIN(T_ARTICLES.CODE_ARTICLE) AS le_minimum_CODE_ARTICLE,	
                T_ARTICLES.ACTIF AS ACTIF
            FROM 
                T_ARTICLES
            WHERE 
                T_ARTICLES.ACTIF = 1
            GROUP BY 
                T_ARTICLES.ACTIF,	
                T_ARTICLES.CODE_PRODUIT
        '''
        return query

    
    def Req_promotions_dt(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_PROMOTIONS.ID_PROMO AS ID_PROMO,	
                T_PROMOTIONS.Date_Debut AS Date_Debut,	
                T_PROMOTIONS.Date_Fin AS Date_Fin,	
                T_CIBLE_PROMOTION.CODE_AGCE AS CODE_AGCE,	
                T_CIBLE_PROMOTION.code_secteur AS code_secteur,	
                T_CIBLE_PROMOTION.CODE_CLIENT AS CODE_CLIENT,	
                T_DT_PROMO_TRANCHE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT
            FROM 
                T_DT_PROMO_TRANCHE,	
                T_PROMOTIONS,	
                T_CIBLE_PROMOTION,	
                T_ARTICLES
            WHERE 
                T_PROMOTIONS.ID_PROMO = T_CIBLE_PROMOTION.ID_PROMO
                AND		T_DT_PROMO_TRANCHE.ID_PROMO = T_PROMOTIONS.ID_PROMO
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_PROMO_TRANCHE.CODE_ARTICLE
                AND
                (
                    T_PROMOTIONS.Date_Debut <= '{Param_dt}'
                    AND	T_PROMOTIONS.Date_Fin >= '{Param_dt}'
                    AND	
                    (
                        T_CIBLE_PROMOTION.code_secteur = 0
                        OR	T_CIBLE_PROMOTION.code_secteur = {param_code_secteur}
                    )
                    AND	
                    (
                        T_CIBLE_PROMOTION.CODE_CLIENT = 0
                        OR	T_CIBLE_PROMOTION.CODE_CLIENT = {param_code_client}
                    )
                )
        '''

        try:
            kwargs = {
                'Param_dt': args[0],
                'param_code_secteur': args[1],
                'param_code_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_qte_commande(self, args):
        query = '''
            SELECT 
                T_COMMANDES.code_secteur AS code_secteur,	
                T_COMMANDES.CODE_CLIENT AS CODE_CLIENT,	
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_COMMANDES.QTE_U AS QTE_U,	
                T_PRODUITS_COMMANDES.QTE_C AS QTE_C
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES
            WHERE 
                T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND
                (
                    T_COMMANDES.code_secteur = {Param_code_secteur}
                    AND	T_COMMANDES.DATE_LIVRAISON = {Param_date_livraison}
                    AND	T_PRODUITS_COMMANDES.CODE_ARTICLE = {Param_code_article}
                    AND	T_COMMANDES.CODE_CLIENT = {Param_code_client}
                )
        '''
        return query.format(**kwargs)

    
    def Req_rapp_ca_pda(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_CHARGEMENT.HEURE_SORTIE AS HEURE_SORTIE,	
                T_CHARGEMENT.HEURE_ENTREE AS HEURE_ENTREE,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                SUM(( ( T_PRODUITS_CHARGEE.TOTAL_VENDU + T_PRODUITS_CHARGEE.CREDIT ) * T_PRODUITS_CHARGEE.PRIX_VNT ) ) AS CA_REALISE,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM * T_PRODUITS_CHARGEE.PRIX ) ) AS MT_RENDUS,	
                SUM(( ( ( T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG + T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP ) + T_PRODUITS_CHARGEE.TOTAL_RENDUS_US ) * T_PRODUITS_CHARGEE.PRIX ) ) AS Autres_rendus,	
                T_SECTEUR.CAT_SECTEUR AS CAT_SECTEUR
            FROM 
                T_CHARGEMENT,	
                T_PRODUITS_CHARGEE,	
                T_OPERATEUR,	
                T_SECTEUR
            WHERE 
                T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND		T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_SECTEUR.CAT_SECTEUR = 1
                )
            GROUP BY 
                T_CHARGEMENT.DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_CHARGEMENT.HEURE_SORTIE,	
                T_CHARGEMENT.HEURE_ENTREE,	
                T_SECTEUR.NOM_SECTEUR,	
                T_SECTEUR.CAT_SECTEUR
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_realisation_globale(self, args):
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SECTEUR.code_secteur AS code_secteur,	
                T_ZONE.NOM_ZONE AS NOM_ZONE,	
                T_BLOC.NOM_BLOC AS NOM_BLOC,	
                SUM(( ( T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE + T_PRODUITS_CHARGEE.QTE_CHARGEE_SUPP ) * T_PRODUITS_CHARGEE.PRIX ) ) AS VAL_CHARGEE,	
                SUM(( ( T_PRODUITS_CHARGEE.TOTAL_VENDU + T_PRODUITS_CHARGEE.CREDIT ) * T_PRODUITS_CHARGEE.PRIX ) ) AS VAL_VENTE,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_VENDU * T_PRODUITS_CHARGEE.PRIX ) ) AS VAL_VNETTE,	
                SUM(( T_PRODUITS_CHARGEE.CREDIT * T_PRODUITS_CHARGEE.PRIX ) ) AS VAL_VCAC,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE * T_PRODUITS_CHARGEE.PRIX ) ) AS VAL_INVENDU,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_RENDUS_POINTE * T_PRODUITS_CHARGEE.PRIX ) ) AS VAL_RENDUS,	
                T_ZONE.CODE_SUPERVISEUR AS CODE_SUPERVISEUR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_CHARGEMENT.CHARGEMENT_CAC AS CHARGEMENT_CAC
            FROM 
                T_ZONE,	
                T_BLOC,	
                T_SECTEUR,	
                T_CHARGEMENT,	
                T_OPERATEUR,	
                T_PRODUITS_CHARGEE,	
                T_PRODUITS,	
                T_ARTICLES
            WHERE 
                T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND		T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_SECTEUR.CODE_BLOC = T_BLOC.CODE_BLOC
                AND		T_BLOC.CODE_ZONE = T_ZONE.CODE_ZONE
                AND		T_PRODUITS_CHARGEE.CODE_ARTICLE = T_ARTICLES.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT BETWEEN {Param_date1} AND {Param_date2}
                    AND	T_PRODUITS.CODE_FAMILLE = {param_code_famille}
                    AND	T_PRODUITS.CODE_PRODUIT = {param_code_produit}
                    AND	T_PRODUITS.CAT_PRODUIT = {param_cat_produit}
                    AND	T_ZONE.CODE_SUPERVISEUR = {param_code_superviseur}
                    AND	T_ZONE.RESP_VENTE = {param_code_resp_vente}
                )
            GROUP BY 
                T_CHARGEMENT.DATE_CHARGEMENT,	
                T_SECTEUR.NOM_SECTEUR,	
                T_ZONE.NOM_ZONE,	
                T_ZONE.CODE_SUPERVISEUR,	
                T_OPERATEUR.FONCTION,	
                T_CHARGEMENT.CHARGEMENT_CAC,	
                T_BLOC.NOM_BLOC,	
                T_SECTEUR.code_secteur
        '''
        return query.format(**kwargs)

    
    def Req_recap_factures_produits(self, args):
        query = '''
            SELECT 
                CAST( T_FACTURE.DATE_HEURE  AS DATE )  AS DATE_FACTURE,	
                CAST( T_FACTURE.DATE_HEURE  AS TIME )  AS HEURE_FACTURE,	
                T_RESP.NOM_OPERATEUR AS NOM_RESP_VENTE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_SUPERVISEUR,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_DT_FACTURE.PRIX AS PRIX,	
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                SUM(T_DT_FACTURE.QTE_VENTE) AS QTE_VENTE,	
                SUM(T_DT_FACTURE.QTE_PERTE) AS QTE_PERTE,	
                SUM(T_DT_FACTURE.QTE_PROMO) AS QTE_PROMO,	
                SUM(T_DT_FACTURE.QTE_REMISE) AS QTE_REMISE
            FROM 
                T_FACTURE,	
                T_DT_FACTURE,	
                T_OPERATEUR,	
                T_OPERATEUR T_RESP,	
                T_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_SECTEUR,	
                T_ARTICLES,	
                T_PRODUITS,	
                T_ZONE,	
                T_BLOC
            WHERE 
                T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_RESP.CODE_OPERATEUR = T_ZONE.RESP_VENTE
                AND		T_OPERATEUR.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_SECTEUR.CODE_BLOC = T_BLOC.CODE_BLOC
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.CODE_CLIENT = {Param_code_client}
                    AND	T_FACTURE.DATE_HEURE BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_ZONE.RESP_VENTE = {Param_code_resp_vente}
                    AND	T_SECTEUR.code_secteur = {Param_code_secteur}
                    AND	T_CLIENTS.CLASSE = {param_eg_classe}
                    AND	T_CLIENTS.CLASSE <> {param_diff_classe}
                    AND	T_PRODUITS.CODE_FAMILLE = {param_code_famille}
                )
            GROUP BY 
                CAST( T_FACTURE.DATE_HEURE  AS DATE ) ,	
                CAST( T_FACTURE.DATE_HEURE  AS TIME ) ,	
                T_FACTURE.CODE_CLIENT,	
                T_DT_FACTURE.PRIX,	
                T_PRODUITS.CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR,	
                T_CLIENTS.NOM_CLIENT,	
                T_SECTEUR.NOM_SECTEUR,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_RESP.NOM_OPERATEUR
            ORDER BY 
                DATE_FACTURE ASC,	
                HEURE_FACTURE ASC,	
                NOM_SECTEUR ASC,	
                NOM_CLIENT ASC,	
                NOM_PRODUIT ASC
        '''
        return query.format(**kwargs)

    
    def Req_recap_factures_produits_cumul(self, args):
        query = '''
            SELECT 
                T_RESP.NOM_OPERATEUR AS NOM_RESP_VENTE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_SUPERVISEUR,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,		
                T_DT_FACTURE.PRIX AS PRIX,	
                T_PRODUITS.CODE_PRODUIT AS CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                SUM(T_DT_FACTURE.QTE_VENTE) AS QTE_VENTE,	
                SUM(T_DT_FACTURE.QTE_PERTE) AS QTE_PERTE,	
                SUM(T_DT_FACTURE.QTE_PROMO) AS QTE_PROMO,	
                SUM(T_DT_FACTURE.QTE_REMISE) AS QTE_REMISE
            FROM 
                T_FACTURE,	
                T_DT_FACTURE,	
                T_OPERATEUR,	
                T_OPERATEUR T_RESP,	
                T_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_SECTEUR,	
                T_ARTICLES,	
                T_PRODUITS,
                T_ZONE,
                T_BLOC
            WHERE 
                T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_SECTEUR.CODE_BLOC = T_BLOC.CODE_BLOC
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR
                AND		T_RESP.CODE_OPERATEUR = T_ZONE.RESP_VENTE
                AND		T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.CODE_CLIENT = {Param_code_client}
                    AND	T_FACTURE.DATE_HEURE BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_ZONE.RESP_VENTE = {Param_code_resp_vente}
                    AND	T_SECTEUR.code_secteur = {Param_code_secteur}
                    AND	T_CLIENTS.CLASSE = {param_eg_classe}
                    AND	T_CLIENTS.CLASSE <> {param_diff_classe}
                )
            GROUP BY 
                T_FACTURE.CODE_CLIENT,	
                T_DT_FACTURE.PRIX,	
                T_PRODUITS.CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR,	
                T_CLIENTS.NOM_CLIENT,	
                T_SECTEUR.NOM_SECTEUR,	
                T_OPERATEUR.NOM_OPERATEUR,
                NOM_RESP_VENTE
            ORDER BY
                NOM_SECTEUR,NOM_CLIENT,NOM_PRODUIT
        '''
        return query.format(**kwargs)

    
    def Req_recap_factures_valeur(self, args):
        query = '''
            SELECT 
                T_RESP.NOM_OPERATEUR AS NOM_RESP_VENTE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_SUPERVISEUR,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,		
                SUM(T_DT_FACTURE.QTE_VENTE * T_DT_FACTURE.PRIX) AS VAL_VENTE,	
                SUM(T_DT_FACTURE.QTE_PERTE * T_DT_FACTURE.PRIX) AS VAL_PERTE,	
                SUM(T_DT_FACTURE.QTE_PROMO * T_DT_FACTURE.PRIX) AS VAL_PROMO,	
                SUM(T_DT_FACTURE.QTE_REMISE * T_DT_FACTURE.PRIX) AS VAL_REMISE
            FROM 
                T_FACTURE,	
                T_DT_FACTURE,	
                T_OPERATEUR,	
                T_OPERATEUR T_RESP,	
                T_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_SECTEUR,	
                T_ARTICLES,	
                T_PRODUITS,
                T_ZONE,
                T_BLOC
            WHERE 
                T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_SECTEUR.CODE_BLOC = T_BLOC.CODE_BLOC
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR
                AND		T_RESP.CODE_OPERATEUR = T_ZONE.RESP_VENTE
                AND		T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.CODE_CLIENT = {Param_code_client}
                    AND	T_FACTURE.DATE_HEURE BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_ZONE.RESP_VENTE = {Param_code_resp_vente}
                    AND	T_SECTEUR.code_secteur = {Param_code_secteur}
                    AND	T_CLIENTS.CLASSE = {param_eg_classe}
                    AND	T_CLIENTS.CLASSE <> {param_diff_classe}
                    AND	T_PRODUITS.CODE_FAMILLE = {param_code_famille}
                )
            GROUP BY 
                
                T_FACTURE.CODE_CLIENT,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR,	
                T_CLIENTS.NOM_CLIENT,	
                T_SECTEUR.NOM_SECTEUR,	
                T_OPERATEUR.NOM_OPERATEUR,
                T_RESP.NOM_OPERATEUR
            ORDER BY
                NOM_SECTEUR,NOM_CLIENT
        '''
        return query.format(**kwargs)

    
    def Req_recap_factures_valeur_date(self, args):
        query = '''
            SELECT 
                CAST( T_FACTURE.DATE_HEURE  AS DATE )  AS DATE_FACTURE,	
                T_RESP.NOM_OPERATEUR AS NOM_RESP_VENTE,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_SUPERVISEUR,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                MIN(T_FACTURE.DATE_HEURE) AS DATE_PR_FACTURE,	
                SUM(( T_DT_FACTURE.QTE_VENTE * T_DT_FACTURE.PRIX ) ) AS VAL_VENTE,	
                SUM(( T_DT_FACTURE.QTE_PERTE * T_DT_FACTURE.PRIX ) ) AS VAL_PERTE,	
                SUM(( T_DT_FACTURE.QTE_PROMO * T_DT_FACTURE.PRIX ) ) AS VAL_PROMO,	
                SUM(( T_DT_FACTURE.QTE_REMISE * T_DT_FACTURE.PRIX ) ) AS VAL_REMISE
            FROM 
                T_FACTURE,	
                T_DT_FACTURE,	
                T_OPERATEUR,	
                T_OPERATEUR T_RESP,	
                T_CLIENTS,	
                T_SOUS_SECTEUR,	
                T_SECTEUR,	
                T_ARTICLES,	
                T_PRODUITS,	
                T_ZONE,	
                T_BLOC
            WHERE 
                T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_RESP.CODE_OPERATEUR = T_ZONE.RESP_VENTE
                AND		T_OPERATEUR.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_CLIENTS.SOUS_SECTEUR
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_SECTEUR.CODE_BLOC = T_BLOC.CODE_BLOC
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.CODE_CLIENT = {Param_code_client}
                    AND	T_FACTURE.DATE_HEURE BETWEEN {Param_dt1} AND {Param_dt2}
                    AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}
                    AND	T_ZONE.RESP_VENTE = {Param_code_resp_vente}
                    AND	T_SECTEUR.code_secteur = {Param_code_secteur}
                    AND	T_CLIENTS.CLASSE = {param_eg_classe}
                    AND	T_CLIENTS.CLASSE <> {param_diff_classe}
                    AND	T_PRODUITS.CODE_FAMILLE = {param_code_famille}
                )
            GROUP BY 
                CAST( T_FACTURE.DATE_HEURE  AS DATE ) ,	
                T_FACTURE.CODE_CLIENT,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR,	
                T_CLIENTS.NOM_CLIENT,	
                T_SECTEUR.NOM_SECTEUR,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_RESP.NOM_OPERATEUR
            ORDER BY 
                NOM_SECTEUR ASC,	
                NOM_CLIENT ASC
        '''
        return query.format(**kwargs)

    
    def Req_recensement_clts_nt(self, args):
        query = '''
            SELECT 
                T_RECENSEMENT.ID_RECENSEMENT AS ID_RECENSEMENT,	
                T_RECENSEMENT.CODE_CLIENT AS CODE_CLIENT,	
                T_RECENSEMENT.NOM_CLIENT AS NOM_CLIENT,	
                T_RECENSEMENT.PRENOM AS PRENOM,	
                T_RECENSEMENT.RAISON_SOCIAL AS RAISON_SOCIAL,	
                T_RECENSEMENT.TEL_CLIENT AS TEL_CLIENT,	
                T_RECENSEMENT.ADRESSE_CLIENT AS ADRESSE_CLIENT,	
                T_RECENSEMENT.QUARTIER AS QUARTIER,	
                T_RECENSEMENT.CODE_GPS AS CODE_GPS,	
                T_RECENSEMENT.ITINERAIRE AS ITINERAIRE,	
                T_RECENSEMENT.SOUS_SECTEUR AS SOUS_SECTEUR,	
                T_RECENSEMENT.TOURNEES AS TOURNEES,	
                T_RECENSEMENT.TYPOLOGIE AS TYPOLOGIE,	
                T_RECENSEMENT.CLASSE_CLIENT AS CLASSE_CLIENT,	
                T_RECENSEMENT.CLIENT_EN_COMPTE AS CLIENT_EN_COMPTE,	
                T_RECENSEMENT.REMISE_COPAG AS REMISE_COPAG,	
                T_RECENSEMENT.REMISE_CONCURENT AS REMISE_CONCURENT,	
                T_RECENSEMENT.VITRINE_COPAG AS VITRINE_COPAG,	
                T_RECENSEMENT.VITRINE_CONCURENT AS VITRINE_CONCURENT,	
                T_RECENSEMENT.TYPE_REGELEMENT AS TYPE_REGELEMENT,	
                T_RECENSEMENT.LONGITUDE_GPS AS LONGITUDE_GPS,	
                T_RECENSEMENT.LATITUDE_GPS AS LATITUDE_GPS,	
                T_RECENSEMENT.NUM_ARRET AS NUM_ARRET,	
                T_RECENSEMENT.DUREE_VISITE AS DUREE_VISITE,	
                T_RECENSEMENT.CODE_TOURNEE AS CODE_TOURNEE,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_CLASSE_CLIENTS.NOM_CLASSE AS NOM_CLASSE,	
                T_CAT_CLIENTS.NOM_CATEGORIE AS NOM_CATEGORIE,	
                T_RECENSEMENT.VALID AS VALID,	
                T_RECENSEMENT.TYPE_PRESENTOIRE AS TYPE_PRESENTOIRE
            FROM 
                T_RECENSEMENT,	
                T_SOUS_SECTEUR,	
                T_SECTEUR,	
                T_CLASSE_CLIENTS,	
                T_CAT_CLIENTS
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_RECENSEMENT.SOUS_SECTEUR
                AND		T_SECTEUR.code_secteur = T_SOUS_SECTEUR.code_secteur
                AND		T_CLASSE_CLIENTS.CODE_CLASSE = T_RECENSEMENT.CLASSE_CLIENT
                AND		T_CAT_CLIENTS.CODE_CAT_CLIENT = T_RECENSEMENT.TYPOLOGIE
                AND
                (
                    T_RECENSEMENT.CODE_CLIENT = {Param_egale}
                    AND	T_RECENSEMENT.CODE_CLIENT <> {Param_diff}
                    AND	T_RECENSEMENT.VALID = {Param_valid}
                )
        '''
        return query.format(**kwargs)

    
    def Req_recherche_client_code_interne(self, args): #Done
        query = '''
            SELECT 
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_CLIENTS.ACTIF AS ACTIF,	
                T_CLIENTS.GROUP_CLIENT AS GROUP_CLIENT,	
                T_CLIENTS.SOLDE_C_STD AS SOLDE_C_STD
            FROM 
                T_CLIENTS
            WHERE 
                T_CLIENTS.ACTIF = 1
                AND	T_CLIENTS.SOLDE_C_STD = {Param_code_in}
        '''
        
        try:
            kwargs = {
                'Param_code_in': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_in'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_recherche_operateur(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.MDP AS MDP,	
                T_OPERATEUR.ACTIF AS ACTIF,	
                T_FONCTION.NOM_FONCTION AS NOM_FONCTION,	
                T_OPERATEUR.FONCTION AS FONCTION
            FROM 
                T_FONCTION,	
                T_OPERATEUR
            WHERE 
                T_FONCTION.CODE_FONCTION = T_OPERATEUR.FONCTION
                AND
                (
                    T_OPERATEUR.ACTIF = 1
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
        '''

        try:
            kwargs = {
                'Param_code_fonction': args[0],
                'Param_nom_operateur': args[1]
            }
        except IndexError as e:
            return e

        kwargs['OPTIONAL_ARG_1'] = '''AND T_OPERATEUR.NOM_OPERATEUR LIKE '%{Param_nom_operateur}%' '''
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_OPERATEUR.FONCTION = {Param_code_fonction}'

        if kwargs['Param_nom_operateur'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
            kwargs['OPTIONAL_ARG_2'] = kwargs['OPTIONAL_ARG_2'][4:]
        
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_fonction'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_recherche_operateur_affectation(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.MDP AS MDP,	
                T_OPERATEUR.ACTIF AS ACTIF,	
                T_FONCTION.NOM_FONCTION AS NOM_FONCTION,	
                T_OPERATEUR.FONCTION AS FONCTION
            FROM 
                T_FONCTION,	
                T_OPERATEUR
            WHERE 
                T_FONCTION.CODE_FONCTION = T_OPERATEUR.FONCTION
                AND
                (
                    T_OPERATEUR.ACTIF = 1
                    AND	T_OPERATEUR.NOM_OPERATEUR LIKE %{Param_nom_operateur}%
                    AND	T_OPERATEUR.FONCTION IN ({Param_fonction}) 
                )
        '''

        try:
            kwargs = {
                'Param_nom_operateur': args[0],
                'Param_fonction': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_recherche_par_matricule(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_OPERATEUR.FONCTION AS FONCTION
            FROM 
                T_OPERATEUR
            WHERE 
                {OPTIONAL_ARG_1}
                T_OPERATEUR.Matricule = {Param_matricule}
        '''

        try:
            kwargs = {
                'Param_fonction': args[0],
                'Param_matricule': args[1]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_matricule'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_OPERATEUR.FONCTION IN ({Param_fonction}) AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_fonction'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_recherche_prevision(self, args): #Done
        query = '''
            SELECT 
                T_PREVISION.Date_Debut AS Date_Debut,	
                T_PREVISION.Date_Fin AS Date_Fin,	
                T_PREVISION.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PREVISION.QTE AS QTE
            FROM 
                T_PREVISION
            WHERE 
                T_PREVISION.Date_Debut <= '{Param_date}'
                AND	T_PREVISION.Date_Fin >= '{Param_date}'
        '''

        try:
            kwargs = {
                'Param_date': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])

        if kwargs['Param_date'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_recherche_secteur(self, args): #Done
        query = '''
            SELECT 
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SECTEUR.code_secteur AS code_secteur
            FROM 
                T_SECTEUR
            WHERE 
                T_SECTEUR.NOM_SECTEUR = {Param_nom_secteur}
        '''
        
        try:
            kwargs = {
                'Param_nom_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_nom_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_reconaissances_operateur(self, args): #Done
        query = '''
            SELECT 
                T_RECONAISSANCES.DATE_RECONAISS AS DATE_RECONAISS,	
                T_RECONAISSANCES.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_RECONAISSANCES.SOLDE_C_STD AS SOLDE_C_STD,	
                T_RECONAISSANCES.SOLDE_P_AG AS SOLDE_P_AG,	
                T_RECONAISSANCES.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_RECONAISSANCES.SOLDE_C_AG AS SOLDE_C_AG,	
                T_RECONAISSANCES.SOLDE_C_PR AS SOLDE_C_PR,	
                T_RECONAISSANCES.SOLDE_C_BLC AS SOLDE_C_BLC,	
                T_RECONAISSANCES.SOLDE_P_EURO AS SOLDE_P_EURO
            FROM 
                T_RECONAISSANCES
            WHERE 
                T_RECONAISSANCES.DATE_RECONAISS = '{Param_date_reconaissance}'
        '''
        
        try:
            kwargs = {
                'Param_date_reconaissance': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_reconaissance'] = self.validateDate(kwargs['Param_date_reconaissance'])

        if kwargs['Param_date_reconaissance'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_regularisation_sans_MS_magasin(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_MOUVEMENTS.MOTIF AS MOTIF,	
                T_MOUVEMENTS.CODE_MAGASIN AS CODE_MAGASIN,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT
            FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                AND	T_MOUVEMENTS.TYPE_MOUVEMENT = 'G'
                AND	T_MOUVEMENTS.MOTIF NOT IN (21, 22) 
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.MOTIF,	
                T_MOUVEMENTS.CODE_MAGASIN,	
                T_MOUVEMENTS.TYPE_PRODUIT
        '''

        try:
            kwargs = {
                'Param_date_mvt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_releve_client_cac(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.NUM_COMMANDE AS NUM_COMMANDE,	
                SUM(T_PRODUITS_LIVREES.MONTANT) AS la_somme_MONTANT,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_GROUP_CLIENTS.NOM_GROUP AS NOM_GROUP,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES,	
                T_CLIENTS,	
                T_GROUP_CLIENTS,	
                T_OPERATEUR
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND		T_LIVRAISON.CODE_CLIENT = T_CLIENTS.CODE_CLIENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_LIVRAISON.code_vendeur
                AND		T_GROUP_CLIENTS.ID_GP_CLIENT = T_CLIENTS.GROUP_CLIENT
                AND
                (
                    T_LIVRAISON.DATE_VALIDATION BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_LIVRAISON.TYPE_MVT IN ('L', 'R') 
                    {OPTIONAL_ARG_1}
                    AND	T_LIVRAISON.STATUT <> 'A'
                    {OPTIONAL_ARG_2}
                )
            GROUP BY 
                T_LIVRAISON.NUM_LIVRAISON,	
                T_LIVRAISON.DATE_VALIDATION,	
                T_LIVRAISON.TYPE_MVT,	
                T_LIVRAISON.NUM_COMMANDE,	
                T_LIVRAISON.CODE_CLIENT,	
                T_GROUP_CLIENTS.NOM_GROUP,	
                T_CLIENTS.NOM_CLIENT,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_LIVRAISON.DATE_LIVRAISON
            ORDER BY 
                TYPE_MVT ASC
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_code_client': args[2],
                'Param_dtl1': args[3],
                'Param_dtl2': args[4]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])
        kwargs['Param_dtl1'] = self.validateDate(kwargs['Param_dtl1'])
        kwargs['Param_dtl2'] = self.validateDate(kwargs['Param_dtl2'])

        for key in ('Param_dt1', 'Param_dt2'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_LIVRAISON.CODE_CLIENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_2'] = '''AND T_LIVRAISON.DATE_LIVRAISON BETWEEN '{Param_dtl1}' AND '{Param_dtl2}' '''

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if None in (kwargs['Param_dtl1'], kwargs['Param_dtl2']) else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_releve_client_details(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.NUM_FACTURE AS NUM_FACTURE,	
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_FACTURE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_FACTURE.DATE_HEURE AS DATE_HEURE,	
                T_FACTURE.MONTANT_FACTURE AS MONTANT_FACTURE,	
                T_FACTURE.MONTANT_PERTE AS MONTANT_PERTE,	
                T_FACTURE.VALID AS VALID,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_DT_FACTURE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_DT_FACTURE.PRIX AS PRIX,	
                T_DT_FACTURE.QTE_VENTE AS QTE_VENTE,	
                T_DT_FACTURE.QTE_PERTE AS QTE_PERTE,	
                T_DT_FACTURE.QTE_PROMO AS QTE_PROMO,	
                T_DT_FACTURE.QTE_REMISE AS QTE_REMISE,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                ( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX )  AS MT,
                ( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX * T_DT_FACTURE.TX_GRATUIT )  AS MT_GRATUIT
            FROM 
                T_ARTICLES,	
                T_DT_FACTURE,	
                T_FACTURE,	
                T_OPERATEUR,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_FACTURE.CODE_OPERATEUR
                AND		T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.CODE_CLIENT IN ({Param_code_client}) 
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            ORDER BY 
                DATE_HEURE ASC,	
                NUM_FACTURE ASC
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_releve_client_details_produits(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.NUM_FACTURE AS NUM_FACTURE,	
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_FACTURE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_FACTURE.DATE_HEURE AS DATE_HEURE,	
                T_FACTURE.MONTANT_FACTURE AS MONTANT_FACTURE,	
                T_FACTURE.MONTANT_PERTE AS MONTANT_PERTE,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_PRODUITS.CODE_PRODUIT AS CODE_ARTICLE,	
                T_PRODUITS.NOM_PRODUIT AS LIBELLE_COURT,	
                MAX(T_DT_FACTURE.PRIX) AS PRIX,	
                SUM(T_DT_FACTURE.QTE_VENTE) AS QTE_VENTE,	
                SUM(T_DT_FACTURE.QTE_PERTE) AS QTE_PERTE,	
                SUM(T_DT_FACTURE.QTE_PROMO) AS QTE_PROMO,	
                SUM(T_DT_FACTURE.QTE_REMISE) AS QTE_REMISE,	
                SUM(( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX ) ) AS MT
            FROM 
                T_ARTICLES,	
                T_DT_FACTURE,	
                T_FACTURE,	
                T_OPERATEUR,	
                T_CLIENTS,	
                T_PRODUITS
            WHERE 
                T_ARTICLES.CODE_PRODUIT = T_PRODUITS.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_OPERATEUR.CODE_OPERATEUR = T_FACTURE.CODE_OPERATEUR
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.CODE_CLIENT IN ({Param_code_client}) 
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_FACTURE.NUM_FACTURE,	
                T_FACTURE.CODE_CLIENT,	
                T_FACTURE.CODE_OPERATEUR,	
                T_FACTURE.DATE_HEURE,	
                T_FACTURE.MONTANT_FACTURE,	
                T_FACTURE.MONTANT_PERTE,	
                T_CLIENTS.NOM_CLIENT,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_PRODUITS.CODE_PRODUIT,	
                T_PRODUITS.NOM_PRODUIT
            ORDER BY 
                DATE_HEURE ASC,	
                NUM_FACTURE ASC
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_dt1': args[1],
                'param_3': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_releve_client_global_produit(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                MIN(T_ARTICLES.RANG) AS le_minimum_RANG,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_DT_FACTURE.PRIX AS PRIX,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                SUM(( ( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX ) - ( ( ( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX ) * T_DT_FACTURE.TX_GRATUIT ) /  100) ) ) AS MT,	
                SUM(T_DT_FACTURE.QTE_VENTE) AS la_somme_QTE_VENTE,	
                SUM(T_DT_FACTURE.QTE_PERTE) AS la_somme_QTE_PERTE,	
                SUM(T_DT_FACTURE.QTE_PROMO) AS la_somme_QTE_PROMO,	
                SUM(T_DT_FACTURE.QTE_REMISE) AS la_somme_QTE_REMISE
            FROM 
                T_FACTURE,	
                T_DT_FACTURE,	
                T_CLIENTS,	
                T_ARTICLES,	
                T_PRODUITS
            WHERE 
                T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.CODE_CLIENT IN ({Param_code_client}) 
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_FACTURE.CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT,	
                T_DT_FACTURE.PRIX,	
                T_PRODUITS.NOM_PRODUIT
            ORDER BY 
                CODE_CLIENT ASC,	
                le_minimum_RANG ASC
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_dt1': args[1],
                'param_3': args[2]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_releve_client_globale(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.NUM_FACTURE AS NUM_FACTURE,	
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_FACTURE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_FACTURE.DATE_HEURE AS DATE_HEURE,	
                T_FACTURE.VALID AS VALID,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                SUM(( ( T_DT_FACTURE.PRIX * ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) ) - ( ( ( T_DT_FACTURE.TX_GRATUIT * T_DT_FACTURE.PRIX ) /  100) * ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) ) ) ) AS MONTANT_FACTURE,	
                SUM(( ( ( ( T_DT_FACTURE.PRIX * ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) ) - ( ( ( T_DT_FACTURE.TX_GRATUIT * T_DT_FACTURE.PRIX ) /  100) * ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) ) ) *  100) / (  100+ T_ARTICLES.TVA ) ) ) AS MONTANT_HT
            FROM 
                T_FACTURE,	
                T_DT_FACTURE,	
                T_OPERATEUR,	
                T_CLIENTS,	
                T_ARTICLES
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_FACTURE.CODE_OPERATEUR
                AND		T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND
                (
                    T_FACTURE.VALID = 1
                    AND	T_FACTURE.CODE_CLIENT = {Param_code_client}
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_FACTURE.NUM_FACTURE,	
                T_FACTURE.CODE_CLIENT,	
                T_FACTURE.CODE_OPERATEUR,	
                T_FACTURE.DATE_HEURE,	
                T_FACTURE.VALID,	
                T_CLIENTS.NOM_CLIENT,	
                T_OPERATEUR.NOM_OPERATEUR
            ORDER BY 
                DATE_HEURE ASC
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_releve_client_tva(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_ARTICLES.TVA AS TVA,	
                SUM(( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX ) ) AS MT,	
                SUM(( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * ( (  100* T_DT_FACTURE.PRIX ) / (  100+ T_ARTICLES.TVA ) ) ) ) AS MT_HT
            FROM 
                T_ARTICLES,	
                T_DT_FACTURE,	
                T_FACTURE,	
                T_CLIENTS
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_CLIENTS.CODE_CLIENT = T_FACTURE.CODE_CLIENT
                AND
                (
                    T_FACTURE.VALID = 1
                    {OPTIONAL_ARG_1}
                    AND	T_FACTURE.DATE_HEURE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_FACTURE.CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT,	
                T_ARTICLES.TVA
            ORDER BY 
                CODE_CLIENT ASC,	
                TVA ASC
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in ('Param_dt1', 'Param_dt2'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_FACTURE.CODE_CLIENT IN ({Param_code_client})'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_releve_dons(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.ORDONATEUR AS ORDONATEUR,	
                T_LIVRAISON.BENEFICIAIRE AS BENEFICIAIRE,	
                SUM(( T_PRODUITS_LIVREES.QTE_CHARGEE * T_PRODUITS_LIVREES.PRIX ) ) AS la_somme_MT
            FROM 
                T_PRODUITS_LIVREES,	
                T_LIVRAISON
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.DATE_LIVRAISON BETWEEN '{Param_Dt1}' AND '{Param_Dt2}'
                    AND	T_LIVRAISON.TYPE_MVT = 'D'
                    {OPTIONAL_ARG_1}
                )
            GROUP BY 
                T_LIVRAISON.DATE_LIVRAISON,	
                T_LIVRAISON.NUM_LIVRAISON,	
                T_LIVRAISON.TYPE_MVT,	
                T_LIVRAISON.ORDONATEUR,	
                T_LIVRAISON.BENEFICIAIRE
        '''
        
        try:
            kwargs = {
                'Param_Dt1': args[0],
                'Param_Dt2': args[1],
                'param_code_sect': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_Dt1'] = self.validateDate(kwargs['Param_Dt1'])
        kwargs['Param_Dt2'] = self.validateDate(kwargs['Param_Dt2'])

        for key in ('Param_Dt1', 'Param_Dt2'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_LIVRAISON.code_secteur = {param_code_sect}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['param_code_sect'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_relve_cac(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON_T_,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_LIVRAISON,	
                T_GROUP_CLIENTS.NOM_GROUP AS ENSEIGNE,	
                T_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_VENDEUR,	
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.NUM_COMMANDE AS NUM_COMMANDE,	
                T_PRODUITS_LIVREES.TYPE_MVT AS TYPE_MVT,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_PRODUITS_LIVREES.PRIX AS PRIX,	
                CASE WHEN ( T_PRODUITS_LIVREES.TYPE_MVT =  'L')  THEN T_PRODUITS_LIVREES.QTE_CHARGEE  ELSE -( T_PRODUITS_LIVREES.QTE_CHARGEE )  END  AS QTE,	
                ( CASE WHEN ( T_PRODUITS_LIVREES.TYPE_MVT =  'L')  THEN T_PRODUITS_LIVREES.QTE_CHARGEE  ELSE -( T_PRODUITS_LIVREES.QTE_CHARGEE )  END * T_PRODUITS_LIVREES.PRIX )  AS MONTANT
            FROM 
                T_GROUP_CLIENTS,	
                T_CLIENTS,	
                T_LIVRAISON,	
                T_PRODUITS_LIVREES,	
                T_ARTICLES,	
                T_OPERATEUR
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRODUITS_LIVREES.CODE_ARTICLE
                AND		T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND		T_LIVRAISON.code_vendeur = T_OPERATEUR.CODE_OPERATEUR
                AND		T_CLIENTS.GROUP_CLIENT = T_GROUP_CLIENTS.ID_GP_CLIENT
                AND		T_CLIENTS.CODE_CLIENT = T_LIVRAISON.CODE_CLIENT
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.TYPE_MVT IN ('L', 'R') 
                    {OPTIONAL_ARG_2}
                )
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_dtj1': args[2],
                'Param_dtj2': args[3]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])
        kwargs['Param_dtj1'] = self.validateDate(kwargs['Param_dtj1'])
        kwargs['Param_dtj2'] = self.validateDate(kwargs['Param_dtj2'])

        kwargs['OPTIONAL_ARG_1'] = '''T_LIVRAISON.DATE_VALIDATION BETWEEN '{Param_dt1}' AND '{Param_dt2}' AND'''
        kwargs['OPTIONAL_ARG_2'] = '''AND	T_LIVRAISON.DATE_LIVRAISON BETWEEN '{Param_dtj1}' AND '{Param_dtj2}' '''

        if kwargs['Param_dt1'] in (None, 'NULL') or kwargs['Param_dt2'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
        
        if kwargs['Param_dtj1'] in (None, 'NULL') or kwargs['Param_dtj2'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_2'] = ''

        return query.format(**kwargs).format(**kwargs)

    
    def Req_remarque_journee(self, args): #Done
        query = '''
            SELECT 
                T_JOURNEE.DATE_JOURNEE AS DATE_JOURNEE,	
                T_JOURNEE.TEMP_MIN AS TEMP_MIN,	
                T_JOURNEE.TEMP_MAX AS TEMP_MAX,	
                T_JOURNEE.PLUV AS PLUV,	
                T_JOURNEE.COMMENTAIRE AS COMMENTAIRE
            FROM 
                T_JOURNEE
            WHERE 
                T_JOURNEE.DATE_JOURNEE = {Param_date_journee}
        '''
                
        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_remise_client(self, args): #Done
        query = '''
            SELECT 
                T_REMISE_CLIENT.Date_Debut AS Date_Debut,	
                T_REMISE_CLIENT.CODE_CLIENT AS CODE_CLIENT,	
                T_REMISE_CLIENT.TX_DERIVES AS TX_DERIVES,	
                T_REMISE_CLIENT.MT_REMISE AS MT_REMISE,	
                T_REMISE_CLIENT.TX_LAIT AS TX_LAIT,	
                T_REMISE_CLIENT.MT_REMISE_LAIT AS MT_REMISE_LAIT
            FROM 
                T_REMISE_CLIENT
            WHERE 
                T_REMISE_CLIENT.Date_Debut = '{Param_DATE_DEBUT}'
                AND	T_REMISE_CLIENT.CODE_CLIENT = {Param_CODE_CLIENT}
        '''

        try:
            kwargs = {
                'Param_DATE_DEBUT': args[0],
                'Param_CODE_CLIENT': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_DATE_DEBUT'] = self.validateDate(kwargs['Param_DATE_DEBUT'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_remise_clt(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_DT_REMISE_CLASSE.TX_REMISE AS TX_REMISE,	
                SUM(( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX ) ) AS CA,	
                T_ARTICLES.TVA AS TVA
            FROM 
                T_DT_FACTURE,	
                T_DT_REMISE_CLASSE,	
                T_FACTURE,	
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_DT_FACTURE.CODE_ARTICLE = T_DT_REMISE_CLASSE.CODE_ARTICLE
                AND
                (
                    T_DT_REMISE_CLASSE.CODE_CLASSE = {Param_code_classe}
                    AND	T_FACTURE.CODE_CLIENT = {Param_code_client}
                    AND	T_FACTURE.DATE_HEURE >= {Param_dt1}
                    AND	T_FACTURE.DATE_HEURE <= {Param_dt2}
                    AND	T_FACTURE.VALID = 1
                )
            GROUP BY 
                T_FACTURE.CODE_CLIENT,	
                T_DT_REMISE_CLASSE.TX_REMISE,	
                T_ARTICLES.TVA
        '''

        try:
            kwargs = {
                'Param_code_classe': args[0],
                'Param_code_client': args[1],
                'Param_dt1': args[2],
                'Param_dt2': args[3]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_remise_clt_produit(self, args): #Done
        query = '''
            SELECT 
                T_FACTURE.CODE_CLIENT AS CODE_CLIENT,	
                T_DT_REMISE_CLASSE.TX_REMISE AS TX_REMISE,	
                SUM(( ( ( T_DT_FACTURE.QTE_VENTE - T_DT_FACTURE.QTE_PERTE ) - T_DT_FACTURE.QTE_PROMO ) * T_DT_FACTURE.PRIX ) ) AS CA,	
                T_ARTICLES.TVA AS TVA,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT
            FROM 
                T_DT_FACTURE,	
                T_DT_REMISE_CLASSE,	
                T_FACTURE,	
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
                AND		T_FACTURE.NUM_FACTURE = T_DT_FACTURE.NUM_FACTURE
                AND		T_DT_FACTURE.CODE_ARTICLE = T_DT_REMISE_CLASSE.CODE_ARTICLE
                AND
                (
                    T_DT_REMISE_CLASSE.CODE_CLASSE = {Param_code_classe}
                    AND	T_FACTURE.CODE_CLIENT = {Param_code_client}
                    AND	T_FACTURE.DATE_HEURE >= '{Param_dt1}'
                    AND	T_FACTURE.DATE_HEURE <= '{Param_dt2}'
                    AND	T_FACTURE.VALID = 1
                )
            GROUP BY 
                T_FACTURE.CODE_CLIENT,	
                T_DT_REMISE_CLASSE.TX_REMISE,	
                T_ARTICLES.TVA,	
                T_ARTICLES.CODE_PRODUIT
        '''

        try:
            kwargs = {
                'Param_code_classe': args[0],
                'Param_code_client': args[1],
                'Param_dt1': args[2],
                'Param_dt2': args[3]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_repartition(self, args): #Done
        query = '''
            SELECT 
                T_REPARTITION.DATE_REPARTITION AS DATE_REPARTITION,	
                T_REPARTITION.CODE_AGENCE AS CODE_AGENCE,	
                T_REPARTITION.LIVRAISONS AS LIVRAISONS,	
                T_REPARTITION.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_REPARTITION.VALIDATION AS VALIDATION,	
                T_REPARTITION.CONTROLEUR_PRODUIT AS CONTROLEUR_PRODUIT,	
                T_REPARTITION.CONTROLEUR_COND AS CONTROLEUR_COND
            FROM 
                T_REPARTITION
            WHERE 
                T_REPARTITION.DATE_REPARTITION = '{Param_date_repartition}'
                AND	T_REPARTITION.CODE_AGENCE = {Param_code_agence}
        '''

        try:
            kwargs = {
                'Param_date_repartition': args[0],
                'Param_code_agence': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_repartition'] = self.validateDate(kwargs['Param_date_repartition'])

        return query.format(**kwargs)

    
    def Req_sit_caisserie(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS.SOUS_TYPE_OPERATION AS SOUS_TYPE_OPERATION,	
                T_OPERATIONS.REF AS REF,	
                T_MOUVEMENTS_CAISSERIE.CODE_CP AS CODE_CP,	
                T_MOUVEMENTS_CAISSERIE.QTE_REEL AS QTE_REEL,	
                T_MOUVEMENTS_CAISSERIE.QTE_THEORIQUE AS QTE_THEORIQUE,	
                T_OPERATIONS.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS.CODE_AGCE1 AS CODE_AGCE1,	
                T_OPERATIONS.CODE_AGCE2 AS CODE_AGCE2,	
                T_Ordre_Mission_Agence.Matricule_Vehicule AS Matricule
            FROM 
                T_Ordre_Mission_Agence,	
                T_OPERATIONS,	
                T_MOUVEMENTS_CAISSERIE
            WHERE 
                T_OPERATIONS.CODE_OPERATION = T_MOUVEMENTS_CAISSERIE.ORIGINE
                AND		T_Ordre_Mission_Agence.Id_Ordre_Mission = T_OPERATIONS.NUM_CONVOYAGE
                AND
                (
                    T_OPERATIONS.TYPE_OPERATION IN ('R', 'T') 
                    AND	T_OPERATIONS.DATE_OPERATION = '{Param_date_reception}'
                )
        '''
        
        try:
            kwargs = {
                'Param_date_reception': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_reception'] = self.validateDate(kwargs['Param_date_reception'])

        if kwargs['Param_date_reception'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_solde_initial_caisse(self, args): #Done
        query = '''
            SELECT 
                T_SOLDE_INITIAL_CAISSE.DATE_JOURNEE AS DATE_JOURNEE,	
                T_SOLDE_INITIAL_CAISSE.CODE_CAISSE AS CODE_CAISSE,	
                T_SOLDE_INITIAL_CAISSE.SOLDE_INITIAL AS SOLDE_INITIAL
            FROM 
                T_SOLDE_INITIAL_CAISSE
            WHERE 
                T_SOLDE_INITIAL_CAISSE.DATE_JOURNEE = '{Param_date_journee}'
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0],
                'Param_code_caisse': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SOLDE_INITIAL_CAISSE.CODE_CAISSE = {Param_code_caisse}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_caisse'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_solde_initial_client(self, args): #Done
        query = '''
            SELECT 
                T_SOLDE_INITIAL_CLIENT.DATE_JOURNEE AS DATE_JOURNEE,	
                T_SOLDE_INITIAL_CLIENT.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.CAT_CLIENT AS CAT_CLIENT,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_C_STD AS SOLDE_C_STD,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_P_AG AS SOLDE_P_AG,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_C_AG AS SOLDE_C_AG,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_C_PR AS SOLDE_C_PR,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_P_EURO AS SOLDE_P_EURO,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_C_BLC AS SOLDE_C_BLC,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_CS1 AS SOLDE_CS1,	
                T_SOLDE_INITIAL_CLIENT.SOLDE_CS2 AS SOLDE_CS2
            FROM 
                T_CLIENTS,	
                T_SOLDE_INITIAL_CLIENT
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_SOLDE_INITIAL_CLIENT.CODE_CLIENT
                AND
                (
                    T_SOLDE_INITIAL_CLIENT.DATE_JOURNEE = '{Param_date_journee}'
                    {OPTIONAL_ARG_1}
                )
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0],
                'Param_client': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SOLDE_INITIAL_CLIENT.CODE_CLIENT = {Param_client}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_solde_operateur(self, args): #Done
        query = '''
            SELECT 
                T_SOLDE_INITIAL.DATE_JOURNEE AS DATE_JOURNEE,	
                T_SOLDE_INITIAL.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_SOLDE_INITIAL.SOLDE_PRODUITS AS SOLDE_PRODUITS,	
                T_SOLDE_INITIAL.SOLDE_CONDITIONNEMENT AS SOLDE_CONDITIONNEMENT,	
                T_SOLDE_INITIAL.MT_VERSER AS MT_VERSER,	
                T_SOLDE_INITIAL.MT_CHEQUES AS MT_CHEQUES,	
                T_SOLDE_INITIAL.TOTAL_VERSER AS TOTAL_VERSER,	
                T_SOLDE_INITIAL.MT_ECART AS MT_ECART,	
                T_SOLDE_INITIAL.SOLDE_C_STD AS SOLDE_C_STD,	
                T_SOLDE_INITIAL.SOLDE_P_AG AS SOLDE_P_AG,	
                T_SOLDE_INITIAL.SOLDE_P_UHT AS SOLDE_P_UHT,	
                T_SOLDE_INITIAL.SOLDE_C_AG AS SOLDE_C_AG,	
                T_SOLDE_INITIAL.SOLDE_C_PR AS SOLDE_C_PR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_SOLDE_INITIAL.MT_A_VERSER AS MT_A_VERSER,	
                T_SOLDE_INITIAL.SOLDE_C_BLC AS SOLDE_C_BLC,	
                T_SOLDE_INITIAL.SOLDE_P_EURO AS SOLDE_P_EURO,	
                T_SOLDE_INITIAL.SOLDE_CS1 AS SOLDE_CS1,	
                T_SOLDE_INITIAL.SOLDE_CS2 AS SOLDE_CS2
            FROM 
                T_OPERATEUR,	
                T_SOLDE_INITIAL
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_SOLDE_INITIAL.CODE_OPERATEUR
                AND
                (
                    T_SOLDE_INITIAL.DATE_JOURNEE = '{Param_date_journee}'
                    {OPTIONAL_ARG_1}
                )
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0],
                'Param_code_operateur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SOLDE_INITIAL.CODE_OPERATEUR = {Param_code_operateur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_operateur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_ss_tournee(self, args): #Done
        query = '''
            SELECT 
                T_TOURNEES_SS.CODE_TOURNEE AS CODE_TOURNEE,	
                T_TOURNEES_SS.CODE_SS AS CODE_SS,	
                T_TOURNEES_SS.LUNDI AS LUNDI,	
                T_TOURNEES_SS.MARDI AS MARDI,	
                T_TOURNEES_SS.MERCREDI AS MERCREDI,	
                T_TOURNEES_SS.JEUDI AS JEUDI,	
                T_TOURNEES_SS.VENDREDI AS VENDREDI,	
                T_TOURNEES_SS.SAMEDI AS SAMEDI,	
                T_TOURNEES_SS.DIMANCHE AS DIMANCHE,	
                T_SOUS_SECTEUR.NOM_SOUS_SECTEUR AS NOM_SOUS_SECTEUR
            FROM 
                T_SOUS_SECTEUR,	
                T_TOURNEES_SS
            WHERE 
                T_SOUS_SECTEUR.CODE_SOUS_SECTEUR = T_TOURNEES_SS.CODE_SS
                AND
                (
                    T_TOURNEES_SS.CODE_TOURNEE = {Param_code_tournee}
                )
        '''

        try:
            kwargs = {
                'Param_code_tournee': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_code_tournee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_stock_article_magasin(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES_MAGASINS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES_MAGASINS.MAGASIN AS MAGASIN,	
                T_ARTICLES_MAGASINS.CATEGORIE AS CATEGORIE,	
                T_ARTICLES_MAGASINS.QTE_STOCK AS QTE_STOCK
            FROM 
                T_ARTICLES_MAGASINS
            WHERE 
                T_ARTICLES_MAGASINS.CODE_ARTICLE = {Param_code_article}
                AND	T_ARTICLES_MAGASINS.MAGASIN = {Param_code_magasin}
                AND	T_ARTICLES_MAGASINS.CATEGORIE = {Param_categorie}
        '''

        try:
            kwargs = {
                'Param_code_article': args[0],
                'Param_code_magasin': args[1],
                'Param_categorie': args[2]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_stock_cond(self, args): #Done
        query = '''
            SELECT 
                T_MAGASIN_COND.CODE_CP AS CODE_CP,	
                SUM(T_MAGASIN_COND.QTE_STOCK) AS la_somme_QTE_STOCK
            FROM 
                T_MAGASINS,	
                T_MAGASIN_COND
            WHERE 
                T_MAGASINS.CODE_MAGASIN = T_MAGASIN_COND.CODE_MAGASIN
                AND
                (
                    T_MAGASIN_COND.CODE_CP = {Param_code_cp}
                    AND	T_MAGASINS.NOM_MAGASIN <> 'HALLE CAISSERIE'
                )
            GROUP BY 
                T_MAGASIN_COND.CODE_CP
        '''

        try:
            kwargs = {
                'Param_code_cp': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_code_cp'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_stock_inital_cond(self, args): #Done
        query = '''
            SELECT 
                T_STOCK_INITI_COND.DATE_JOURNEE AS DATE_JOURNEE,	
                T_STOCK_INITI_COND.CODE_CP AS CODE_CP,	
                SUM(T_STOCK_INITI_COND.STOCK_INIT) AS la_somme_STOCK_INIT
            FROM 
                T_STOCK_INITI_COND
            WHERE 
                T_STOCK_INITI_COND.DATE_JOURNEE = '{Param_date_journee}'
            GROUP BY 
                T_STOCK_INITI_COND.DATE_JOURNEE,	
                T_STOCK_INITI_COND.CODE_CP
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_stock_inital_cond_magasin(self, args): #Done
        query = '''
            SELECT 
                T_STOCK_INITI_COND.DATE_JOURNEE AS DATE_JOURNEE,	
                T_STOCK_INITI_COND.CODE_CP AS CODE_CP,	
                T_STOCK_INITI_COND.CODE_MAGASIN AS CODE_MAGASIN,	
                SUM(T_STOCK_INITI_COND.STOCK_INIT) AS la_somme_STOCK_INIT
            FROM 
                T_STOCK_INITI_COND
            WHERE 
                T_STOCK_INITI_COND.DATE_JOURNEE = '{Param_date_journee}'
            GROUP BY 
                T_STOCK_INITI_COND.DATE_JOURNEE,	
                T_STOCK_INITI_COND.CODE_CP,	
                T_STOCK_INITI_COND.CODE_MAGASIN
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_stock_initial(self, args): #Done
        query = '''
            SELECT 
                T_STOCK_INIT.DATE_PS AS DATE_PS,	
                T_STOCK_INIT.CODE_ARTICLE AS CODE_ARTICLE,	
                T_STOCK_INIT.CATEGORIE AS CATEGORIE,	
                SUM(T_STOCK_INIT.QTE_INIT) AS la_somme_QTE_INIT
            FROM 
                T_STOCK_INIT
            WHERE 
                T_STOCK_INIT.CATEGORIE = {Param_categorie}
                AND	T_STOCK_INIT.DATE_PS = '{Param_date_stock}'
            GROUP BY 
                T_STOCK_INIT.DATE_PS,	
                T_STOCK_INIT.CODE_ARTICLE,	
                T_STOCK_INIT.CATEGORIE
        '''

        try:
            kwargs = {
                'Param_categorie': args[0],
                'Param_date_stock': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_stock'] = self.validateDate(kwargs['Param_date_stock'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_stock_initial_magasins(self, args): #Done
        query = '''
            SELECT 
                T_STOCK_INIT.DATE_PS AS DATE_PS,	
                T_STOCK_INIT.CODE_ARTICLE AS CODE_ARTICLE,	
                T_STOCK_INIT.CODE_MAGASIN AS CODE_MAGASIN,	
                T_STOCK_INIT.CATEGORIE AS CATEGORIE,	
                T_STOCK_INIT.QTE_INIT AS QTE_INIT
            FROM 
                T_STOCK_INIT
            WHERE 
                T_STOCK_INIT.DATE_PS = '{Param_date}'
        '''
        
        try:
            kwargs = {
                'Param_date': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])

        if kwargs['Param_date'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_stock_produit_magasin(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES_MAGASINS.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_ARTICLES_MAGASINS.QTE_STOCK) AS la_somme_QTE_STOCK
            FROM 
                T_ARTICLES_MAGASINS
            WHERE 
                T_ARTICLES_MAGASINS.CATEGORIE = 'PRODUIT'
                {OPTIONAL_ARG_1}
            GROUP BY 
                T_ARTICLES_MAGASINS.CODE_ARTICLE
            HAVING 
                SUM(T_ARTICLES_MAGASINS.QTE_STOCK) <> 0
        '''

        try:
            kwargs = {
                'Paramcode_magasin': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_ARTICLES_MAGASINS.MAGASIN = {Paramcode_magasin}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Paramcode_magasin'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_stock_recep_temp(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_REEL) AS la_somme_QTE_REEL
            FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.TYPE_MOUVEMENT = 'X'
                AND	T_MOUVEMENTS.CODE_ARTICLE = {Param_code_article}
            GROUP BY 
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.TYPE_MOUVEMENT
        '''

        try:
            kwargs = {
                'Param_code_article': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_article'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_sup_chargement_secteur(self, args): #Done
        query = '''
            DELETE FROM 
                T_CHARGEMENT
            WHERE 
                T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_supp_bl_mission(self, args): #Done
        query = '''
            DELETE FROM 
                T_Mission_BL
            WHERE 
                T_Mission_BL.Id_Det = {Param_id_det}
        '''

        try:
            kwargs = {
                'Param_id_det': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_det'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_budget(self, args): #Done
        query = '''
            DELETE FROM 
                T_BUDGET_MENSUEL
            WHERE 
                T_BUDGET_MENSUEL.DATE_BUDGET = '{Param_date_budget}'
        '''
        
        try:
            kwargs = {
                'Param_date_budget': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_budget'] = self.validateDate(kwargs['Param_date_budget'])

        if kwargs['Param_date_budget'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_chargement_cac(self, args): #Done
        query = '''
            DELETE FROM 
                T_CHARGEMENT
            WHERE 
                T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                AND	T_CHARGEMENT.CHARGEMENT_CAC = 1
                AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_supp_cond_livree(self, args): #Done
        query = '''
            DELETE FROM 
                T_COND_LIVRAISON
            WHERE 
                T_COND_LIVRAISON.NUM_LIVRAISON = {Param_nbl}
        '''

        try:
            kwargs = {
                'Param_nbl': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_nbl'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_dt_reclamation(self, args): #Done
        query = '''
            DELETE FROM 
                T_DT_RECLAMATION
            WHERE 
                T_DT_RECLAMATION.ID_RECLAMATION = {Param_id_reclamation}
        '''

        try:
            kwargs = {
                'Param_id_reclamation': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_reclamation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_hist_clients(self, args): #Done
        query = '''
            DELETE FROM 
                T_MOY_VENTE_CLIENTS
            WHERE 
                T_MOY_VENTE_CLIENTS.DATE_VENTE = '{Param_date_journee}'
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_hist_secteur(self, args): #Done
        query = '''
            DELETE FROM 
                T_MOY_VENTE_ARTICLE
            WHERE 
                T_MOY_VENTE_ARTICLE.DATE_VENTE = '{Param_date_journee}'
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_initial_client(self, args): #Done
        query = '''
            DELETE FROM 
                T_SOLDE_INITIAL_CLIENT
            WHERE 
                T_SOLDE_INITIAL_CLIENT.DATE_JOURNEE = '{Param_date_journee}'
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)


    def Req_supp_itinéraire(self, args): #Done
        query = '''
            DELETE FROM 
                T_ITINERAIRES
            WHERE 
                T_ITINERAIRES.CODE_TOURNEE = {Param_code_tournee}
        '''

        try:
            kwargs = {
                'Param_code_tournee': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_tournee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_justification_caisserie(self, args): #Done
        query = '''
            DELETE FROM 
                T_AUTORISATION_SOLDE_CAISSERIE
            WHERE 
                T_AUTORISATION_SOLDE_CAISSERIE.ID_JUSTIFICATION = {Param_id_justification}
        '''

        try:
            kwargs = {
                'Param_id_justification': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_justification'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_justification_solde(self, args): #Done
        query = '''
            DELETE FROM 
                T_AUTORISATIONS_SOLDE
            WHERE 
                T_AUTORISATIONS_SOLDE.ID_JUSTIFICATION = {Param_id_justification}
        '''

        try:
            kwargs = {
                'Param_id_justification': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_justification'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_mouvement_operation(self, args): #Done
        query = '''
            DELETE FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.ORIGINE = {Param_code_operation}
        '''

        try:
            kwargs = {
                'Param_code_operation': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_operation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_moy_vente(self, args): #Done
        query = '''
            DELETE FROM 
                T_MOY_VENTE_ARTICLE
            WHERE 
                T_MOY_VENTE_ARTICLE.DATE_VENTE = '{Param_date}'
        '''

        try:
            kwargs = {
                'Param_date': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])

        if kwargs['Param_date'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_moy_vente_clients(self, args): #Done
        query = '''
            DELETE FROM 
                T_MOY_VENTE_CLIENTS
            WHERE 
                T_MOY_VENTE_CLIENTS.DATE_VENTE = '{Param_date}'
        '''

        try:
            kwargs = {
                'Param_date': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])

        if kwargs['Param_date'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_mvente(self, args): #Done
        query = '''
            DELETE FROM 
                T_MOYENNE_VENTE
            WHERE 
                T_MOYENNE_VENTE.DATE_VENTE = '{Param_date_mvente}'
        '''

        try:
            kwargs = {
                'Param_date_mvente': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_mvente'] = self.validateDate(kwargs['Param_date_mvente'])

        if kwargs['Param_date_mvente'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_mvt_caisse(self, args): #Done
        query = '''
            DELETE FROM 
                T_MOUVEMENTS_CAISSE
            WHERE 
                T_MOUVEMENTS_CAISSE.ORIGINE = {Param_code_operation}
        '''

        try:
            kwargs = {
                'Param_code_operation': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_operation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_mvt_caisserie(self, args): #Done
        query = '''
            DELETE FROM 
                T_MOUVEMENTS_CAISSERIE
            WHERE 
                T_MOUVEMENTS_CAISSERIE.ORIGINE = {Param_code_operation}
        '''

        try:
            kwargs = {
                'Param_code_operation': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_operation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_obj_clients(self, args): #Done
        query = '''
            DELETE FROM 
                T_OBJECTIF_CLIENTS
            WHERE 
                T_OBJECTIF_CLIENTS.DATE_OBJECTIF = '{Param_date}'
        '''
        
        try:
            kwargs = {
                'Param_date': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])

        if kwargs['Param_date'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_obj_secteurs(self, args): #Done
        query = '''
            DELETE FROM 
                T_OBJECTIF_SECTEURS
            WHERE 
                T_OBJECTIF_SECTEURS.DATE_OBJECTIF = '{Param_date}'
        '''
        
        try:
            kwargs = {
                'Param_date': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])

        if kwargs['Param_date'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_objectif(self, args): #Done
        query = '''
            DELETE FROM 
                T_OBJECTIF_VENTE
            WHERE 
                T_OBJECTIF_VENTE.DATE_OBJECTIF = '{Param_date_objectif}'
        '''

        try:
            kwargs = {
                'Param_date_objectif': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_objectif'] = self.validateDate(kwargs['Param_date_objectif'])

        if kwargs['Param_date_objectif'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_objectif_agence(self, args): #Done
        query = '''
            DELETE FROM 
                T_OBJECTIF_AGENCE
            WHERE 
                T_OBJECTIF_AGENCE.DATE_OBJECTIF = '{Param_date_objectif}'
        '''

        try:
            kwargs = {
                'Param_date_objectif': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_objectif'] = self.validateDate(kwargs['Param_date_objectif'])

        if kwargs['Param_date_objectif'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_objectif_clients(self, args): #Done
        query = '''
            DELETE FROM 
                T_OBJECTIF_CLIENTS
            WHERE 
                T_OBJECTIF_CLIENTS.DATE_OBJECTIF = '{Param_date_journee}'
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0],
                'Param_code_client': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_OBJECTIF_CLIENTS.CODE_CLIENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_supp_objectif_rendus(self, args): #Done
        query = '''
            DELETE FROM 
                T_OBJECTIF_RENDUS
            WHERE 
                T_OBJECTIF_RENDUS.DATE_OBJECTIF = '{Param_date_objectif}'
        '''

        try:
            kwargs = {
                'Param_date_objectif': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_objectif'] = self.validateDate(kwargs['Param_date_objectif'])

        if kwargs['Param_date_objectif'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_prevision(self, args): #Done
        query = '''
            DELETE FROM 
                T_PREVISION
            WHERE 
                T_PREVISION.Date_Debut = '{Param_date_debut}'
        '''

        try:
            kwargs = {
                'Param_date_debut': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_debut'] = self.validateDate(kwargs['Param_date_debut'])

        if kwargs['Param_date_debut'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_produit_comm(self, args): #Done
        query = '''
            DELETE FROM 
                T_PRODUITS_COMMANDES
            WHERE 
                T_PRODUITS_COMMANDES.ID_COMMANDE = {Param_id_commande}
        '''

        try:
            kwargs = {
                'Param_id_commande': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_id_commande'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_produits_chargees(self, args): #Done
        query = '''
            DELETE FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.code_secteur = {Param_code_secteur}
                AND	T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}'
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_chargement': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_supp_produits_livree(self, args): #Done
        query = '''
            DELETE FROM 
                T_PRODUITS_LIVREES
            WHERE 
                T_PRODUITS_LIVREES.NUM_LIVRAISON = {Param_nbl}
        '''

        try:
            kwargs = {
                'Param_nbl': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_nbl'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_remise(self, args): #Done
        query = '''
            DELETE FROM 
                T_REMISE_CLIENT
            WHERE 
                T_REMISE_CLIENT.Date_Debut = '{Param_date_debut}'
                {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_date_debut': args[0],
                'Param_code_client': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_debut'] = self.validateDate(kwargs['Param_date_debut'])

        if kwargs['Param_nbl'] in (None, 'NULL'):
            return ValueError
        

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_REMISE_CLIENT.CODE_CLIENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_supp_solde_caisse(self, args): #Done
        query = '''
            DELETE FROM 
                T_SOLDE_INITIAL_CAISSE
            WHERE 
                T_SOLDE_INITIAL_CAISSE.DATE_JOURNEE = '{Param_date_journee}'
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_solde_init_caisse(self, args): #Done
        query = '''
            DELETE FROM 
                T_SOLDE_INITIAL_CAISSE
            WHERE 
                T_SOLDE_INITIAL_CAISSE.DATE_JOURNEE = '{Param_date_journee}'
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_solde_init_client(self, args): #Done
        query = '''
            DELETE FROM 
                T_SOLDE_INITIAL_CLIENT
            WHERE 
                T_SOLDE_INITIAL_CLIENT.DATE_JOURNEE = '{Param_date_journee}'
                {Param_code_client}
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0],
                'Param_code_client': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_SOLDE_INITIAL_CLIENT.CODE_CLIENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_supp_solde_initial_operateur(self, args): #Done
        query = '''
            DELETE FROM 
                T_SOLDE_INITIAL
            WHERE 
                T_SOLDE_INITIAL.DATE_JOURNEE = '{Param_date_journee}'
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_statistique(self, args): #Done
        query = '''
            DELETE FROM 
                T_STATISTIQUES
            WHERE 
                T_STATISTIQUES.DATE_JOURNEE = '{Param_date_journee}'
                {OPTIONAL_ARG_1}
                AND	
                (
                    (
                        T_STATISTIQUES.CODE_CLIENT = 0
                        AND	T_STATISTIQUES.CATEGORIE = 'GMS'
                    )
                    OR	T_STATISTIQUES.CATEGORIE = 'SEC'
                    OR	T_STATISTIQUES.CATEGORIE = 'DEP'
                )
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_STATISTIQUES.code_secteur = {Param_code_secteur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_supp_statistiques_clients(self, args): #Done
        query = '''
            DELETE FROM 
                T_STATISTIQUES
            WHERE 
                T_STATISTIQUES.DATE_JOURNEE = '{Param_date_journee}'
                AND	T_STATISTIQUES.CODE_CLIENT <> 0
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_statistiques_stock(self, args): #Done
        query = '''
            DELETE FROM 
                T_STATISTIQUES_STOCK
            WHERE 
                T_STATISTIQUES_STOCK.DATE_JOURNEE = '{Param_date_journee}'
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_supp_stock_init_cond(self, args): #Done
        query = '''
            DELETE FROM 
                T_STOCK_INITI_COND
            WHERE 
                T_STOCK_INITI_COND.DATE_JOURNEE = '{Param_date_journee}'
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)

    
    def Req_supp_stock_initial(self, args): #Done
        query = '''
            DELETE FROM 
                T_STOCK_INIT
            WHERE 
                T_STOCK_INIT.DATE_PS = '{Param_date_journee}'
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def req_susp_cond_chargement_journee(self, args): #Done
        query = '''
            SELECT 
                T_COND_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                SUM(( T_COND_CHARGEE.QTE_CHARGEE_VAL - T_COND_CHARGEE.QTE_POINTE ) ) AS AVOIR,	
                SUM(( ( T_COND_CHARGEE.QTE_POINTE + T_COND_CHARGEE.QTE_CHAR_SUPP ) - T_COND_CHARGEE.QTE_RETOUR ) ) AS SUSP,	
                SUM(T_COND_CHARGEE.CREDIT) AS la_somme_CREDIT,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_CHARGEMENT.CHARGEMENT_CAC AS CHARGEMENT_CAC,	
                T_CHARGEMENT.VALID AS VALID
            FROM 
                T_CHARGEMENT,	
                T_COND_CHARGEE,	
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_COND_CHARGEE.CODE_OPERATEUR
                AND		T_COND_CHARGEE.CODE_CHARGEMENT = T_CHARGEMENT.CODE_CHARGEMENT
                AND
                (
                    T_COND_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}'
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
            GROUP BY 
                T_COND_CHARGEE.DATE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_COND,	
                T_OPERATEUR.FONCTION,	
                T_CHARGEMENT.CHARGEMENT_CAC,	
                T_CHARGEMENT.VALID
        '''

        try:
            kwargs = {
                'req_susp_cond_chargement_journee': args[0],
                'Param_operateur': args[1],
                'Param_code_cp': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['req_susp_cond_chargement_journee'] = self.validateDate(kwargs['req_susp_cond_chargement_journee'])

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_COND_CHARGEE.CODE_OPERATEUR = {Param_operateur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_operateur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_COND_CHARGEE.CODE_COND = {Param_code_cp}'
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_cp'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_susp_emballage(self, args): #Done
        query = '''
            SELECT 
                T_JOURNEE.DATE_JOURNEE AS DATE_JOURNEE,	
                T_JOURNEE.AS_C_STD AS AS_C_STD,	
                T_JOURNEE.AS_C_AG AS AS_C_AG,	
                T_JOURNEE.AS_C_PR AS AS_C_PR,	
                T_JOURNEE.AS_P_AG AS AS_P_AG,	
                T_JOURNEE.AS_P_UHT AS AS_P_UHT,	
                T_JOURNEE.NS_C_STD AS NS_C_STD,	
                T_JOURNEE.NS_P_AG AS NS_P_AG,	
                T_JOURNEE.NS_P_UHT AS NS_P_UHT,	
                T_JOURNEE.NS_C_AG AS NS_C_AG,	
                T_JOURNEE.NS_C_PR AS NS_C_PR,	
                T_JOURNEE.AS_P_EURO AS AS_P_EURO,	
                T_JOURNEE.AS_CS_BLC AS AS_CS_BLC,	
                T_JOURNEE.NS_PAL_EURO AS NS_PAL_EURO,	
                T_JOURNEE.NS_CS_BLC AS NS_CS_BLC,	
                T_JOURNEE.AS_CS1 AS AS_CS1,	
                T_JOURNEE.AS_CS2 AS AS_CS2,	
                T_JOURNEE.NV_CS1 AS NV_CS1,	
                T_JOURNEE.NV_CS2 AS NV_CS2
            FROM 
                T_JOURNEE
            WHERE 
                T_JOURNEE.DATE_JOURNEE = '{Param_date_journee}'
        '''

        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_synthese_livraison_date(self, args): #Done
        query = '''
            SELECT 
                T_SYNTHESE_LIVRAISON.DATE_JOURNEE AS DATE_JOURNEE,	
                T_SYNTHESE_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_SYNTHESE_LIVRAISON.CODE_AGCE AS CODE_AGCE,	
                T_SYNTHESE_LIVRAISON.PROGRAMME AS PROGRAMME,	
                T_SYNTHESE_LIVRAISON.COMMANDE AS COMMANDE,	
                T_SYNTHESE_LIVRAISON.LIVRE AS LIVRE,	
                T_SYNTHESE_LIVRAISON.MOTIF_NON_COMMANDE AS MOTIF_NON_COMMANDE,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT
            FROM 
                T_CLIENTS,	
                T_SYNTHESE_LIVRAISON
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_SYNTHESE_LIVRAISON.CODE_CLIENT
                AND
                (
                    T_SYNTHESE_LIVRAISON.DATE_JOURNEE = '{Param_DATE_JOURNEE}'
                )
        '''

        try:
            kwargs = {
                'Param_DATE_JOURNEE': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_DATE_JOURNEE'] = self.validateDate(kwargs['Param_DATE_JOURNEE'])
        
        if kwargs['Param_DATE_JOURNEE'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_tache_operateur(self, args): #Done
        query = '''
            SELECT 
                T_OPERTEURS_TACHES.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_OPERTEURS_TACHES.ID_TACHE AS ID_TACHE
            FROM 
                T_OPERTEURS_TACHES
            WHERE 
                T_OPERTEURS_TACHES.CODE_OPERATEUR = {Param_code_operateur}
        '''

        try:
            kwargs = {
                'Param_code_operateur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_operateur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_almientation_caisse2(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                SUM(T_OPERATIONS_CAISSE.MONTANT) AS la_somme_MONTANT
            FROM 
                T_OPERATIONS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.DATE_VALIDATION = '{Param_date_validation}'
                AND	T_OPERATIONS_CAISSE.TYPE_OPERATION = 'A'
            GROUP BY 
                T_OPERATIONS_CAISSE.TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_autorisation_caisserie(self, args): #Done
        query = '''
            SELECT 
                T_AUTORISATION_SOLDE_CAISSERIE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_AUTORISATION_SOLDE_CAISSERIE.CS_STD AS CS_STD,	
                T_AUTORISATION_SOLDE_CAISSERIE.CS_PR AS CS_PR,	
                T_AUTORISATION_SOLDE_CAISSERIE.CS_AG AS CS_AG,	
                T_AUTORISATION_SOLDE_CAISSERIE.CS_BLC AS CS_BLC,	
                T_AUTORISATION_SOLDE_CAISSERIE.PAL_AG AS PAL_AG,	
                T_AUTORISATION_SOLDE_CAISSERIE.PAL_UHT AS PAL_UHT,	
                T_AUTORISATION_SOLDE_CAISSERIE.PAL_EURO AS PAL_EURO
            FROM 
                T_AUTORISATION_SOLDE_CAISSERIE
            WHERE 
                T_AUTORISATION_SOLDE_CAISSERIE.DATE_HEURE <= '{Param_dt}'
                AND	T_AUTORISATION_SOLDE_CAISSERIE.DATE_ECHU >= '{Param_dt}'
        '''

        try:
            kwargs = {
                'Param_dt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])

        if kwargs['Param_dt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_avoir(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.COMPTE_ECART AS COMPTE_ECART,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                SUM(( T_PRODUITS_CHARGEE.QTE_ECART * T_PRODUITS_CHARGEE.PRIX ) ) AS Expr1
            FROM 
                T_CHARGEMENT,	
                T_PRODUITS_CHARGEE,	
                T_OPERATEUR
            WHERE 
                T_PRODUITS_CHARGEE.COMPTE_ECART = T_OPERATEUR.CODE_OPERATEUR
                AND		T_PRODUITS_CHARGEE.CODE_CHARGEMENT = T_CHARGEMENT.CODE_CHARGEMENT
                AND
                (
                    T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}'
                    AND	T_PRODUITS_CHARGEE.QTE_ECART <> 0
                )
            GROUP BY 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.COMPTE_ECART,	
                T_OPERATEUR.FONCTION
        '''
        
        try:
            kwargs = {
                'Param_date_chargement': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_avoir_mvt(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.COMPTE_ECART AS COMPTE_ECART,	
                SUM(T_MOUVEMENTS.MONTANT_ECART) AS la_somme_MONTANT_ECART,	
                T_OPERATEUR.FONCTION AS FONCTION
            FROM 
                T_OPERATEUR,	
                T_MOUVEMENTS
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_MOUVEMENTS.COMPTE_ECART
                AND
                (
                    T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                )
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.COMPTE_ECART,	
                T_OPERATEUR.FONCTION
        '''
        
        try:
            kwargs = {
                'Param_date_mvt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_chargement(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_VENDU) AS la_somme_TOTAL_VENDU,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE) AS la_somme_TOTAL_INVENDU_POINTE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM) AS la_somme_TOTAL_RENDUS_COM,	
                SUM(T_PRODUITS_CHARGEE.CREDIT) AS la_somme_CREDIT
            FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
            GROUP BY 
                T_PRODUITS_CHARGEE.CODE_ARTICLE,	
                T_PRODUITS_CHARGEE.code_secteur
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_chargement_supp_secteur(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_LIVREES.code_secteur AS code_secteur,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_LIVREES.DATE_VALIDATION AS DATE_VALIDATION,	
                T_PRODUITS_LIVREES.TYPE_MVT AS TYPE_MVT,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE
            FROM 
                T_PRODUITS_LIVREES
            WHERE 
                T_PRODUITS_LIVREES.code_secteur = {Param_code_secteur}
                AND	T_PRODUITS_LIVREES.TYPE_MVT = 'C'
                AND	T_PRODUITS_LIVREES.DATE_VALIDATION = '{Param_date_validation}'
            GROUP BY 
                T_PRODUITS_LIVREES.code_secteur,	
                T_PRODUITS_LIVREES.CODE_ARTICLE,	
                T_PRODUITS_LIVREES.DATE_VALIDATION,	
                T_PRODUITS_LIVREES.TYPE_MVT
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_cmd_secteur(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COMMANDES.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_ARTICLES.LIBELLE AS LIBELLE,	
                T_ARTICLES.QTE_PACK AS QTE_PACK,	
                SUM(T_PRODUITS_COMMANDES.QTE_U) AS la_somme_QTE_U,	
                SUM(( T_PRODUITS_COMMANDES.QTE_U / T_ARTICLES.QTE_PACK ) ) AS la_somme_QTE_PACK,	
                SUM(T_PRODUITS_COMMANDES.QTE_C) AS la_somme_QTE_C
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES,	
                T_SECTEUR,	
                T_ARTICLES,	
                T_PRODUITS
            WHERE 
                T_ARTICLES.CODE_PRODUIT = T_PRODUITS.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_COMMANDES.CODE_ARTICLE
                AND		T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND		T_SECTEUR.code_secteur = T_COMMANDES.code_secteur
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_COMMANDES.TYPE_COMMANDE <> 'U'
                    {OPTIONAL_ARG_2}
                )
            GROUP BY 
                T_COMMANDES.DATE_LIVRAISON,	
                T_COMMANDES.code_secteur,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE,	
                T_SECTEUR.NOM_SECTEUR,	
                T_ARTICLES.LIBELLE,	
                T_ARTICLES.QTE_PACK
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'param_lst_famille': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])
        kwargs['OPTIONAL_ARG_1'] = '''T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}' AND'''
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_PRODUITS.CODE_FAMILLE IN ({param_lst_famille})'

        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_livraison'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['param_lst_famille'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_commande_usine(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.TYPE_COMMANDE AS TYPE_COMMANDE,	
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_COMMANDES.QTE_U) AS la_somme_QTE_U,	
                SUM(T_PRODUITS_COMMANDES.QTE_C) AS la_somme_QTE_C,	
                SUM(T_PRODUITS_COMMANDES.QTE_P) AS la_somme_QTE_P
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES
            WHERE 
                T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND
                (
                    T_COMMANDES.TYPE_COMMANDE = 'U'
                    AND	T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}'
                    {OPTIONAL_ARG_1}
                )
            GROUP BY 
                T_COMMANDES.TYPE_COMMANDE,	
                T_COMMANDES.DATE_LIVRAISON,	
                T_PRODUITS_COMMANDES.CODE_ARTICLE
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_article': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_PRODUITS_COMMANDES.CODE_ARTICLE = {Param_code_article}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_article'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_commandes(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_COMMANDES.QTE_U) AS la_somme_QTE_U,	
                SUM(T_PRODUITS_COMMANDES.QTE_P) AS la_somme_QTE_P,	
                SUM(T_PRODUITS_COMMANDES.QTE_C) AS la_somme_QTE_C
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES
            WHERE 
                T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_COMMANDES.TYPE_COMMANDE IN ('S', 'C') 
                )
            GROUP BY 
                T_PRODUITS_COMMANDES.CODE_ARTICLE
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])
        kwargs['OPTIONAL_ARG_1'] = '''T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}' AND'''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_livraison'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_commandes_periode(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_COMMANDES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_COMMANDES.QTE_U) AS la_somme_QTE_U,	
                SUM(T_PRODUITS_COMMANDES.QTE_C) AS la_somme_QTE_C
            FROM 
                T_COMMANDES,	
                T_PRODUITS_COMMANDES
            WHERE 
                T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND
                (
                    T_COMMANDES.DATE_LIVRAISON BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_COMMANDES.TYPE_COMMANDE <> 'U'
                )
            GROUP BY 
                T_PRODUITS_COMMANDES.CODE_ARTICLE
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_total_cond_charg_supp_secteur(self, args): #Done
        query = '''
            SELECT 
                T_COND_LIVRAISON.code_secteur AS code_secteur,	
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                T_COND_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_COND_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                SUM(T_COND_LIVRAISON.QTE_CHARGEE) AS la_somme_QTE_CHARGEE
            FROM 
                T_COND_LIVRAISON
            WHERE 
                T_COND_LIVRAISON.code_secteur = {Param_code_secteur}
                AND	T_COND_LIVRAISON.TYPE_MVT = 'C'
                AND	T_COND_LIVRAISON.DATE_VALIDATION = '{Param_date_validation}'
            GROUP BY 
                T_COND_LIVRAISON.code_secteur,	
                T_COND_LIVRAISON.CODE_CP,	
                T_COND_LIVRAISON.DATE_VALIDATION,	
                T_COND_LIVRAISON.TYPE_MVT
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_cond_retour_secteur(self, args): #Done
        query = '''
            SELECT 
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                T_COND_LIVRAISON.code_secteur AS code_secteur,	
                T_COND_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_COND_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                SUM(T_COND_LIVRAISON.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                SUM(T_COND_LIVRAISON.VALEUR_CHARGEE) AS la_somme_VALEUR_CHARGEE
            FROM 
                T_LIVRAISON,	
                T_COND_LIVRAISON
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_COND_LIVRAISON.NUM_LIVRAISON
                AND
                (
                    T_COND_LIVRAISON.code_secteur = {Param_code_secteur}
                    AND	T_COND_LIVRAISON.TYPE_MVT = 'R'
                    AND	T_COND_LIVRAISON.DATE_VALIDATION = '{Param_date_validation}'
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
            GROUP BY 
                T_COND_LIVRAISON.code_secteur,	
                T_COND_LIVRAISON.TYPE_MVT,	
                T_COND_LIVRAISON.DATE_VALIDATION,	
                T_COND_LIVRAISON.CODE_CP
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_conseig_decons(self, args): #Done
        query = '''
            SELECT 
                T_REGELEMENT_COND.CODE_OPERTAEUR AS CODE_OPERTAEUR,	
                T_REGELEMENT_COND.DATE_VALIDATION AS DATE_VALIDATION,	
                T_REGELEMENT_COND.REGLER_C_STD AS REGLER_C_STD,	
                T_REGELEMENT_COND.REGLER_P_AG AS REGLER_P_AG,	
                T_REGELEMENT_COND.REGLER_P_UHT AS REGLER_P_UHT,	
                T_REGELEMENT_COND.REGLER_C_AG AS REGLER_C_AG,	
                T_REGELEMENT_COND.REGLER_C_PR AS REGLER_C_PR,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_REGELEMENT_COND.REGLER_C_BLC AS REGLER_C_BLC,	
                T_REGELEMENT_COND.REGLER_P_EURO AS REGLER_P_EURO,	
                T_REGELEMENT_COND.REGLER_CS1 AS REGLER_CS1,	
                T_REGELEMENT_COND.REGLER_CS2 AS REGLER_CS2
            FROM 
                T_OPERATEUR,	
                T_REGELEMENT_COND
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_REGELEMENT_COND.CODE_OPERTAEUR
                AND
                (
                    T_REGELEMENT_COND.DATE_VALIDATION = '{Param_date_validation}'
                )
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_conseigne_mag_operateur(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_OPERATEUR.Matricule AS Matricule,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                SUM(T_COND_LIVRAISON.QTE_CHARGEE) AS la_somme_QTE_CHARGEE
            FROM 
                T_OPERATEUR,	
                T_LIVRAISON,	
                T_COND_LIVRAISON,	
                T_CHARGEMENT
            WHERE 
                T_CHARGEMENT.code_secteur = T_LIVRAISON.code_secteur
                AND	T_CHARGEMENT.DATE_CHARGEMENT = T_LIVRAISON.DATE_LIVRAISON
                AND		T_CHARGEMENT.code_vendeur = T_OPERATEUR.CODE_OPERATEUR
                AND		T_LIVRAISON.NUM_LIVRAISON = T_COND_LIVRAISON.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.CODE_CLIENT = {Param_code_client}
                    AND	T_LIVRAISON.DATE_LIVRAISON BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_COND_LIVRAISON.CODE_CP = 1
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
            GROUP BY 
                T_LIVRAISON.DATE_LIVRAISON,	
                T_LIVRAISON.CODE_CLIENT,	
                T_COND_LIVRAISON.CODE_CP,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_OPERATEUR.Matricule,	
                T_LIVRAISON.TYPE_MVT
            ORDER BY 
                TYPE_MVT ASC
        '''

        try:
            kwargs = {
                'Param_code_client': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_credit_secteur(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_LIVREES.code_secteur AS code_secteur,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                SUM(T_PRODUITS_LIVREES.MONTANT) AS la_somme_MONTANT,	
                T_PRODUITS_LIVREES.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_PRODUITS_LIVREES.code_secteur = {Param_code_secteur}
                    AND	T_PRODUITS_LIVREES.TYPE_MVT = 'L'
                    AND	T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.DATE_VALIDATION = '{Param_date_validation}'
                )
            GROUP BY 
                T_PRODUITS_LIVREES.CODE_ARTICLE,	
                T_PRODUITS_LIVREES.code_secteur,	
                T_PRODUITS_LIVREES.TYPE_MVT,	
                T_LIVRAISON.DATE_VALIDATION
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_credit_secteur_cond(self, args): #Done
        query = '''
            SELECT 
                T_COND_LIVRAISON.code_secteur AS code_secteur,	
                T_COND_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                SUM(T_COND_LIVRAISON.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                SUM(T_COND_LIVRAISON.VALEUR_CHARGEE) AS la_somme_VALEUR_CHARGEE,	
                T_COND_LIVRAISON.TYPE_MVT AS TYPE_MVT
            FROM 
                T_LIVRAISON,	
                T_COND_LIVRAISON
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_COND_LIVRAISON.NUM_LIVRAISON
                AND
                (
                    T_COND_LIVRAISON.code_secteur = {Param_code_secteur}
                    AND	T_COND_LIVRAISON.DATE_VALIDATION = '{Param_date_validation}'
                    AND	T_COND_LIVRAISON.TYPE_MVT = 'L'
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
            GROUP BY 
                T_COND_LIVRAISON.code_secteur,	
                T_COND_LIVRAISON.CODE_CP,	
                T_COND_LIVRAISON.DATE_VALIDATION,	
                T_COND_LIVRAISON.TYPE_MVT
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_decompte(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_DECOMPTE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_DECOMPTE.REGLEMENT AS REGLEMENT,	
                SUM(T_DECOMPTE.MONTANT) AS la_somme_MONTANT,	
                T_OPERATEUR.FONCTION AS FONCTION
            FROM 
                T_OPERATEUR,	
                T_DECOMPTE
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_DECOMPTE.CODE_OPERATEUR
                AND
                (
                    T_DECOMPTE.DATE_VALIDATION = '{Param_date_validation}'
                    AND	T_DECOMPTE.MODE_PAIEMENT <> 'R'
                )
            GROUP BY 
                T_DECOMPTE.CODE_OPERATEUR,	
                T_DECOMPTE.DATE_VALIDATION,	
                T_DECOMPTE.REGLEMENT,	
                T_OPERATEUR.FONCTION
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_decompte_espece(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                T_DECOMPTE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_DECOMPTE.REGLEMENT AS REGLEMENT,	
                SUM(T_DECOMPTE.MONTANT) AS la_somme_MONTANT,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT
            FROM 
                T_OPERATEUR,	
                T_DECOMPTE
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_DECOMPTE.CODE_OPERATEUR
                AND
                (
                    T_DECOMPTE.DATE_VALIDATION = '{Param_date_validation}'
                    AND	T_DECOMPTE.MODE_PAIEMENT = 'E'
                )
            GROUP BY 
                T_DECOMPTE.CODE_OPERATEUR,	
                T_DECOMPTE.DATE_VALIDATION,	
                T_DECOMPTE.REGLEMENT,	
                T_OPERATEUR.FONCTION,	
                T_DECOMPTE.MODE_PAIEMENT
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_depense_categorie(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_MOUVEMENTS_CAISSE.CODE_CAISSE AS CODE_CAISSE,	
                SUM(T_OPERATIONS_CAISSE.MONTANT) AS la_somme_MONTANT,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.NATURE AS NATURE
            FROM 
                T_OPERATIONS_CAISSE,	
                T_MOUVEMENTS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.CODE_OPERATION = T_MOUVEMENTS_CAISSE.ORIGINE
                AND
                (
                    T_OPERATIONS_CAISSE.TYPE_OPERATION IN ({Param_type_operation}) 
                    AND	T_MOUVEMENTS_CAISSE.CODE_CAISSE = {Param_code_caisse}
                    AND	T_OPERATIONS_CAISSE.DATE_VALIDATION = '{Param_date_validation}'
                )
            GROUP BY 
                T_OPERATIONS_CAISSE.TYPE_OPERATION,	
                T_MOUVEMENTS_CAISSE.CODE_CAISSE,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.NATURE
        '''

        try:
            kwargs = {
                'Param_type_operation': args[0],
                'Param_code_caisse': args[1],
                'Param_date_validation': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_don(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                T_LIVRAISON.LIVRAISON_TOURNEE AS LIVRAISON_TOURNEE,	
                T_LIVRAISON.Type_Livraison AS Type_Livraison
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_LIVRAISON.TYPE_MVT = 'D'
                    AND	T_LIVRAISON.LIVRAISON_TOURNEE <> 1
                )
            GROUP BY 
                T_LIVRAISON.DATE_LIVRAISON,	
                T_PRODUITS_LIVREES.CODE_ARTICLE,	
                T_LIVRAISON.TYPE_MVT,	
                T_LIVRAISON.LIVRAISON_TOURNEE,	
                T_LIVRAISON.Type_Livraison
        '''
        
        try:
            kwargs = {
                'Param_date_livraison': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_dons_secteur(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_LIVREES.code_secteur AS code_secteur,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_LIVREES.TYPE_MVT AS TYPE_MVT,	
                T_PRODUITS_LIVREES.DATE_VALIDATION AS DATE_VALIDATION,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_PRODUITS_LIVREES.TYPE_MVT = 'D'
                    AND	T_PRODUITS_LIVREES.code_secteur = {Param_code_secteur}
                    AND	T_PRODUITS_LIVREES.DATE_VALIDATION = '{Param_date_validation}'
                    AND	T_LIVRAISON.Type_Livraison = 1
                )
            GROUP BY 
                T_PRODUITS_LIVREES.code_secteur,	
                T_PRODUITS_LIVREES.CODE_ARTICLE,	
                T_PRODUITS_LIVREES.TYPE_MVT,	
                T_PRODUITS_LIVREES.DATE_VALIDATION
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_ecart_chargement(self, args): #Done
        query = '''
            SELECT 
                T_COND_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_COND_CHARGEE.COMPTE_ECART AS COMPTE_ECART,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                SUM(T_COND_CHARGEE.ECART) AS la_somme_ECART
            FROM 
                T_COND_CHARGEE
            WHERE 
                T_COND_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}'
                {OPTIONAL_ARG_1}
            GROUP BY 
                T_COND_CHARGEE.DATE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_COND,	
                T_COND_CHARGEE.COMPTE_ECART
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_cp': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_COND_CHARGEE.CODE_COND = {Param_code_cp}'
        kwargs['OPTIONAL_ARG_1'] = ''  if kwargs['Param_code_cp'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_ecart_cond(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS_CAISSERIE.CODE_CP AS CODE_CP,	
                SUM(T_MOUVEMENTS_CAISSERIE.QTE_ECART) AS la_somme_QTE_ECART,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_MOUVEMENTS_CAISSERIE.COMPTE_ECART AS COMPTE_ECART
            FROM 
                T_OPERATIONS,	
                T_MOUVEMENTS_CAISSERIE,	
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_MOUVEMENTS_CAISSERIE.COMPTE_ECART
                AND		T_OPERATIONS.CODE_OPERATION = T_MOUVEMENTS_CAISSERIE.ORIGINE
                AND
                (
                    T_OPERATIONS.DATE_OPERATION = '{Param_date_mvt}'
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
            GROUP BY 
                T_MOUVEMENTS_CAISSERIE.CODE_CP,	
                T_OPERATEUR.FONCTION,	
                T_MOUVEMENTS_CAISSERIE.COMPTE_ECART
        '''

        try:
            kwargs = {
                'Param_date_mvt': args[0],
                'Param2': args[1],
                'Param3': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_MOUVEMENTS_CAISSERIE.CODE_CP = {Param_code_cp}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_cp'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_MOUVEMENTS_CAISSERIE.COMPTE_ECART = {Param_operateur}'
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_operateur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_ecart_inventaires(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT
            FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                AND	T_MOUVEMENTS.TYPE_MOUVEMENT = 'I'
                AND	T_MOUVEMENTS.TYPE_PRODUIT = 'PRODUIT'
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.TYPE_PRODUIT
        '''
        
        try:
            kwargs = {
                'Param_date_mvt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_livraison(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                SUM(T_PRODUITS_LIVREES.QTE_IMPORTE) AS la_somme_QTE_IMPORTE
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
            GROUP BY 
                T_LIVRAISON.DATE_LIVRAISON,	
                T_PRODUITS_LIVREES.CODE_ARTICLE
        '''
        
        try:
            kwargs = {
                'Param_date_livraison': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_livraison_client(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.Type_Livraison AS Type_Livraison,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_LIVREES.CODE_CLIENT AS CODE_CLIENT,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.DATE_VALIDATION BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_LIVRAISON.TYPE_MVT IN ('L', 'R') 
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
            GROUP BY 
                T_LIVRAISON.Type_Livraison,	
                T_PRODUITS_LIVREES.CODE_ARTICLE,	
                T_PRODUITS_LIVREES.CODE_CLIENT,	
                T_LIVRAISON.TYPE_MVT
            ORDER BY 
                TYPE_MVT ASC
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_livraison_sec_gms(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                SUM(T_PRODUITS_LIVREES.QTE_IMPORTE) AS la_somme_QTE_IMPORTE,	
                T_SECTEUR.CAT_SECTEUR AS CAT_SECTEUR
            FROM 
                T_SECTEUR,	
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.code_secteur = T_SECTEUR.code_secteur
                AND		T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_LIVRAISON.STATUT <> 'A'
                    AND	T_SECTEUR.CAT_SECTEUR = 2
                )
            GROUP BY 
                T_LIVRAISON.DATE_LIVRAISON,	
                T_PRODUITS_LIVREES.CODE_ARTICLE,	
                T_SECTEUR.CAT_SECTEUR
        '''
        
        try:
            kwargs = {
                'Param_date_livraison': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_montant_cheques(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_DECOMPTE.MODE_PAIEMENT AS MODE_PAIEMENT,	
                SUM(T_DECOMPTE.MONTANT) AS la_somme_MONTANT
            FROM 
                T_DECOMPTE
            WHERE 
                T_DECOMPTE.MODE_PAIEMENT = 'C'
                AND	T_DECOMPTE.DATE_VALIDATION = '{Param_date_validation}'
            GROUP BY 
                T_DECOMPTE.DATE_VALIDATION,	
                T_DECOMPTE.MODE_PAIEMENT
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_mvt_caisse(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS_CAISSE.CODE_CAISSE AS CODE_CAISSE,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                SUM(T_MOUVEMENTS_CAISSE.MONTANT) AS la_somme_MONTANT,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION
            FROM 
                T_OPERATIONS_CAISSE,	
                T_MOUVEMENTS_CAISSE
            WHERE 
                T_OPERATIONS_CAISSE.CODE_OPERATION = T_MOUVEMENTS_CAISSE.ORIGINE
                AND
                (
                    T_OPERATIONS_CAISSE.DATE_OPERATION = '{Param_date_operation}'
                    AND	T_OPERATIONS_CAISSE.DATE_VALIDATION = '{Param_date_validation}'
                )
            GROUP BY 
                T_MOUVEMENTS_CAISSE.CODE_CAISSE,	
                T_OPERATIONS_CAISSE.DATE_OPERATION,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION
        '''

        try:
            kwargs = {
                'Param_date_operation': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_operation'] = self.validateDate(kwargs['Param_date_operation'])
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_mvt_chargement(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE_VAL) AS la_somme_QTE_CHARGEE_VAL,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE) AS la_somme_QTE_CHARGEE_POINTE,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE_SUPP) AS la_somme_QTE_CHARGEE_SUPP,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE) AS la_somme_TOTAL_INVENDU_POINTE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG) AS la_somme_TOTAL_RENDUS_AG,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_US) AS la_somme_TOTAL_RENDUS_US,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM) AS la_somme_TOTAL_RENDUS_COM,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP) AS la_somme_TOTAL_RENDUS_SP,	
                SUM(T_PRODUITS_CHARGEE.QTE_ECART) AS la_somme_QTE_ECART,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_VENDU) AS la_somme_TOTAL_VENDU,	
                SUM(T_PRODUITS_CHARGEE.CREDIT) AS la_somme_CREDIT,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_GRATUIT) AS la_somme_TOTAL_GRATUIT,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_DONS) AS la_somme_TOTAL_DONS,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_ECHANGE) AS la_somme_TOTAL_ECHANGE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_REMISE) AS la_somme_TOTAL_REMISE,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                T_CHARGEMENT.VALID AS VALID,	
                T_OPERATEUR.FONCTION AS FONCTION
            FROM 
                T_CHARGEMENT,	
                T_PRODUITS_CHARGEE,	
                T_OPERATEUR
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND
                (
                    T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}'
                )
            GROUP BY 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT,	
                T_PRODUITS_CHARGEE.CODE_ARTICLE,	
                T_CHARGEMENT.VALID,	
                T_OPERATEUR.FONCTION
        '''
        
        try:
            kwargs = {
                'Param_date_chargement': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_mvt_cond(self, args):
        query = '''
            SELECT 
                T_OPERATIONS.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS.TYPE_OPERATION AS TYPE_OPERATION,	
                T_MOUVEMENTS_CAISSERIE.CODE_CP AS CODE_CP,	
                SUM(T_MOUVEMENTS_CAISSERIE.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_OPERATIONS.SOUS_TYPE_OPERATION AS SOUS_TYPE_OPERATION,	
                T_OPERATIONS.CODE_AGCE1 AS CODE_AGCE1,	
                T_OPERATIONS.CODE_AGCE2 AS CODE_AGCE2
            FROM 
                T_OPERATIONS,	
                T_MOUVEMENTS_CAISSERIE
            WHERE 
                T_OPERATIONS.CODE_OPERATION = T_MOUVEMENTS_CAISSERIE.ORIGINE
                AND
                (
                    T_OPERATIONS.DATE_OPERATION = {Param_date_operation}
                    AND	T_OPERATIONS.TYPE_OPERATION = {Param_type_operation}
                    AND	T_OPERATIONS.SOUS_TYPE_OPERATION = {Param_sous_type}
                )
            GROUP BY 
                T_OPERATIONS.DATE_OPERATION,	
                T_OPERATIONS.TYPE_OPERATION,	
                T_MOUVEMENTS_CAISSERIE.CODE_CP,	
                T_OPERATIONS.SOUS_TYPE_OPERATION,	
                T_OPERATIONS.CODE_AGCE1,	
                T_OPERATIONS.CODE_AGCE2
        '''
        return query.format(**kwargs)

    
    def Req_total_mvt_cond_cac(self, args): #Done
        query = '''
            SELECT 
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                T_COND_LIVRAISON.TYPE_CLIENT AS TYPE_CLIENT,	
                T_COND_LIVRAISON.QTE_CHARGEE AS QTE_CHARGEE,	
                T_COND_LIVRAISON.QTE_IMPORTE AS QTE_IMPORTE,	
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.STATUT AS STATUT,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION_T_
            FROM 
                T_LIVRAISON,	
                T_COND_LIVRAISON
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_COND_LIVRAISON.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.TYPE_MVT = {Param_type_mvt}
                    AND	T_LIVRAISON.STATUT <> 'A'
                    AND	T_LIVRAISON.DATE_VALIDATION = '{Param_date_val}'
                )
        '''

        try:
            kwargs = {
                'Param_type_mvt': args[0],
                'Param_date_val': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_val'] = self.validateDate(kwargs['Param_date_val'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_mvt_operation(self, args):
        query = '''
            SELECT 
                T_OPERATIONS.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS.SOUS_TYPE_OPERATION AS SOUS_TYPE_OPERATION,	
                T_MOUVEMENTS_CAISSERIE.CODE_CP AS CODE_CP,	
                T_MOUVEMENTS_CAISSERIE.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                SUM(T_MOUVEMENTS_CAISSERIE.QTE_REEL) AS la_somme_QTE_REEL,	
                SUM(T_MOUVEMENTS_CAISSERIE.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                SUM(T_MOUVEMENTS_CAISSERIE.QTE_ECART) AS la_somme_QTE_ECART,	
                T_OPERATIONS.MOTIF AS MOTIF
            FROM 
                T_OPERATIONS,	
                T_MOUVEMENTS_CAISSERIE
            WHERE 
                T_OPERATIONS.CODE_OPERATION = T_MOUVEMENTS_CAISSERIE.ORIGINE
                AND
                (
                    T_OPERATIONS.DATE_OPERATION = {Param_date_operation}
                    AND	T_MOUVEMENTS_CAISSERIE.TYPE_MOUVEMENT = {Param_type_mouvement}
                    AND	T_OPERATIONS.SOUS_TYPE_OPERATION = {Param_sous_type}
                    AND	T_OPERATIONS.MOTIF IN ({Param_ls_motifs}) 
                )
            GROUP BY 
                T_OPERATIONS.DATE_OPERATION,	
                T_MOUVEMENTS_CAISSERIE.CODE_CP,	
                T_MOUVEMENTS_CAISSERIE.TYPE_MOUVEMENT,	
                T_OPERATIONS.SOUS_TYPE_OPERATION,	
                T_OPERATIONS.MOTIF
        '''
        return query.format(**kwargs)

    
    def Req_total_mvt_vente(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                T_PRODUITS_CHARGEE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_CHARGEE.PRIX AS PRIX,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_CHARGEE * T_PRODUITS_CHARGEE.PRIX ) ) AS la_somme_TOTAL_CHARGEE,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM * T_PRODUITS_CHARGEE.PRIX ) ) AS la_somme_TOTAL_RENDUS_COM,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE * T_PRODUITS_CHARGEE.PRIX ) ) AS la_somme_TOTAL_INVENDU_POINTE,	
                SUM(T_PRODUITS_CHARGEE.MONTANT) AS la_somme_MONTANT,	
                SUM(T_PRODUITS_CHARGEE.MONTANT_CREDIT) AS la_somme_MONTANT_CREDIT
            FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.code_secteur = {Param_code_secteur}
                AND	T_PRODUITS_CHARGEE.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
            GROUP BY 
                T_PRODUITS_CHARGEE.code_secteur,	
                T_PRODUITS_CHARGEE.CODE_ARTICLE,	
                T_PRODUITS_CHARGEE.PRIX
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_position_cond(self, args): #Done
        query = '''
            SELECT 
                T_MAGASIN_COND.CODE_CP AS CODE_CP,	
                SUM(T_MAGASIN_COND.QTE_STOCK) AS la_somme_QTE_STOCK
            FROM 
                T_MAGASIN_COND
            GROUP BY 
                T_MAGASIN_COND.CODE_CP
        '''
        return query

    
    def Req_total_prelev(self, args): #Done
        query = '''
            SELECT 
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR AS CODE_OPERATEUR,	
                SUM(( T_DT_PRELEVEMENT_COND.SOLDE_STD - T_DT_PRELEVEMENT_COND.DECON_STD ) ) AS la_somme_SOLDE_STD
            FROM 
                T_PRELEVEMENT_SUSP_COND,	
                T_DT_PRELEVEMENT_COND
            WHERE 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV = T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT
                AND
                (
                    T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION <> '1900-01-01 00:00:00'
                )
            GROUP BY 
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR
        '''
        return query

    
    def Req_total_prelev_journalier(self, args): #Done
        query = '''
            SELECT 
                T_OPERATEUR.FONCTION AS FONCTION,	
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION AS DATE_VALIDATION,	
                SUM(T_DT_PRELEVEMENT_COND.SUSP_VENTE) AS la_somme_SUSP_VENTE,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_STD) AS la_somme_SOLDE_STD,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_AGR) AS la_somme_SOLDE_AGR,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_PRM) AS la_somme_SOLDE_PRM,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_PAG) AS la_somme_SOLDE_PAG,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_PUHT) AS la_somme_SOLDE_PUHT,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_STD) AS la_somme_DECON_STD,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_AGR) AS la_somme_DECON_AGR,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_PRM) AS la_somme_DECON_PRM,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_PAG) AS la_somme_DECON_PAG,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_PUHT) AS la_somme_DECON_PUHT,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_STD) AS la_somme_PRIME_STD,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_AGR) AS la_somme_PRIME_AGR,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_PRM) AS la_somme_PRIME_PRM,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_PAG) AS la_somme_PRIME_PAG,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_PUHT) AS la_somme_PRIME_PUHT,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_EURO) AS la_somme_SOLDE_EURO,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_CBL) AS la_somme_SOLDE_CBL,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_EURO) AS la_somme_DECON_EURO,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_CBL) AS la_somme_DECON_CBL,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_CS1) AS la_somme_DECON_CS1,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_CS1) AS la_somme_SOLDE_CS1
            FROM 
                T_PRELEVEMENT_SUSP_COND,	
                T_DT_PRELEVEMENT_COND,	
                T_OPERATEUR
            WHERE 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV = T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT
                AND		T_OPERATEUR.CODE_OPERATEUR = T_DT_PRELEVEMENT_COND.CODE_OPERATEUR
                AND
                (
                    T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION = '{Param_date_validation}'
                )
            GROUP BY 
                T_OPERATEUR.FONCTION,	
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_prelevement(self, args): #Done
        query = '''
            SELECT 
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION AS DATE_VALIDATION,	
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR AS CODE_OPERATEUR,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_STD) AS la_somme_SOLDE_STD,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_AGR) AS la_somme_SOLDE_AGR,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_PRM) AS la_somme_SOLDE_PRM,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_PAG) AS la_somme_SOLDE_PAG,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_PUHT) AS la_somme_SOLDE_PUHT,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_STD) AS la_somme_DECON_STD,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_AGR) AS la_somme_DECON_AGR,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_PRM) AS la_somme_DECON_PRM,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_PAG) AS la_somme_DECON_PAG,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_PUHT) AS la_somme_DECON_PUHT,	
                SUM(T_DT_PRELEVEMENT_COND.SUSP_VENTE) AS la_somme_SUSP_VENTE,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_STD) AS la_somme_PRIME_STD,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_AGR) AS la_somme_PRIME_AGR,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_PAG) AS la_somme_PRIME_PAG,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_PRM) AS la_somme_PRIME_PRM,	
                SUM(T_DT_PRELEVEMENT_COND.PRIME_PUHT) AS la_somme_PRIME_PUHT,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_CBL) AS la_somme_DECON_CBL,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_EURO) AS la_somme_DECON_EURO,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_CBL) AS la_somme_SOLDE_CBL,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_EURO) AS la_somme_SOLDE_EURO,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_CS1) AS la_somme_SOLDE_CS1,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_CS2) AS la_somme_SOLDE_CS2,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_CS1) AS la_somme_DECON_CS1,	
                SUM(T_DT_PRELEVEMENT_COND.DECON_CS2) AS la_somme_DECON_CS2
            FROM 
                T_PRELEVEMENT_SUSP_COND,	
                T_DT_PRELEVEMENT_COND
            WHERE 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV = T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT
                AND
                (
                    T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION = '{Param_date_validation}'
                )
            GROUP BY 
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR,	
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION
        '''
        
        try:
            kwargs = {
                'Param_date_validation': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        if kwargs['Param_date_validation'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_qte_chargement_cond(self, args): #Done
        query = '''
            SELECT 
                T_COND_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_COND AS CODE_COND,	
                SUM(T_COND_CHARGEE.QTE_CHARGEE_VAL) AS la_somme_QTE_CHARGEE_VAL,	
                SUM(T_COND_CHARGEE.QTE_CHAR_SUPP) AS la_somme_QTE_CHAR_SUPP,	
                SUM(T_COND_CHARGEE.QTE_RETOUR) AS la_somme_QTE_RETOUR,	
                SUM(T_COND_CHARGEE.ECART) AS la_somme_ECART,	
                SUM(T_COND_CHARGEE.QTE_CHARGEE) AS la_somme_QTE_CHARGEE,	
                T_CHARGEMENT.VALID AS VALID
            FROM 
                T_CHARGEMENT,	
                T_COND_CHARGEE
            WHERE 
                T_CHARGEMENT.CODE_CHARGEMENT = T_COND_CHARGEE.CODE_CHARGEMENT
                AND
                (
                    T_COND_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}'
                )
            GROUP BY 
                T_COND_CHARGEE.DATE_CHARGEMENT,	
                T_COND_CHARGEE.CODE_COND,	
                T_CHARGEMENT.VALID
        '''
        
        try:
            kwargs = {
                'Param_date_chargement': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_reception(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_CAISSE) AS la_somme_QTE_CAISSE,	
                SUM(T_MOUVEMENTS.QTE_PAL) AS la_somme_QTE_PAL
            FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.TYPE_MOUVEMENT = 'R'
                AND	T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                {OPTIONAL_ARG_1}
            GROUP BY 
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.DATE_MVT
        '''

        try:
            kwargs = {
                'Param_date_mvt': args[0],
                'param_2': args[1],
                'param_3': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_MOUVEMENTS.CODE_ARTICLE = {Param_code_article}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_article'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_reconaissances(self, args): #Done
        query = '''
            SELECT 
                T_RECONAISSANCES.DATE_RECONAISS AS DATE_RECONAISS,	
                SUM(T_RECONAISSANCES.SOLDE_C_STD) AS la_somme_SOLDE_C_STD,	
                SUM(T_RECONAISSANCES.SOLDE_P_AG) AS la_somme_SOLDE_P_AG,	
                SUM(T_RECONAISSANCES.SOLDE_P_UHT) AS la_somme_SOLDE_P_UHT,	
                SUM(T_RECONAISSANCES.SOLDE_C_AG) AS la_somme_SOLDE_C_AG,	
                SUM(T_RECONAISSANCES.SOLDE_C_PR) AS la_somme_SOLDE_C_PR,	
                SUM(T_RECONAISSANCES.SOLDE_C_BLC) AS la_somme_SOLDE_C_BLC,	
                SUM(T_RECONAISSANCES.SOLDE_P_EURO) AS la_somme_SOLDE_P_EURO
            FROM 
                T_RECONAISSANCES
            WHERE 
                {OPTIONAL_ARG_1}
                T_RECONAISSANCES.DATE_RECONAISS <= '{Param_date_max}'
            GROUP BY 
                T_RECONAISSANCES.DATE_RECONAISS
        '''

        try:
            kwargs = {
                'Param_date_reconaissance': args[0],
                'Param_date_max': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_reconaissance'] = self.validateDate(kwargs['param'])
        kwargs['Param_date_max'] = self.validateDate(kwargs['Param_date_max'])

        kwargs['OPTIONAL_ARG_1'] = '''T_RECONAISSANCES.DATE_RECONAISS = '{Param_date_reconaissance}' AND'''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_reconaissance'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_reglement(self, args): #Done
        query = '''
            SELECT 
                T_DECOMPTE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_DECOMPTE.REGLEMENT AS REGLEMENT,	
                SUM(T_DECOMPTE.MONTANT) AS la_somme_MONTANT
            FROM 
                T_DECOMPTE
            WHERE 
                T_DECOMPTE.REGLEMENT = {Param_type_reg} AND
                T_DECOMPTE.DATE_VALIDATION = '{Param_date_reglement}'
                AND	T_DECOMPTE.MODE_PAIEMENT <> 'R'
            GROUP BY 
                T_DECOMPTE.DATE_VALIDATION,	
                T_DECOMPTE.REGLEMENT
        '''

        try:
            kwargs = {
                'Param_type_reg': args[0],
                'Param_date_reglement': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_reglement'] = self.validateDate(kwargs['Param_date_reglement'])

        kwargs['OPTIONAL_ARG_1'] = 'T_DECOMPTE.REGLEMENT = {Param_type_reg} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_type_reg'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_regularisation_MS(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_MAGASIN AS CODE_MAGASIN,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT,	
                T_MOUVEMENTS.MOTIF AS MOTIF
            FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                AND	T_MOUVEMENTS.TYPE_MOUVEMENT = 'G'
                AND	T_MOUVEMENTS.TYPE_PRODUIT = 'PRODUIT'
                {OPTIONAL_ARG_1}
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT,	
                T_MOUVEMENTS.MOTIF,	
                T_MOUVEMENTS.CODE_MAGASIN
        '''

        try:
            kwargs = {
                'Param_date_mvt': args[0],
                'Param_motif': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_MOUVEMENTS.MOTIF IN ({Param_motif})'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_motif'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_remise_ca(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                SUM(( T_PRODUITS_CHARGEE.TOTAL_VENDU * ( T_PRODUITS_CHARGEE.PRIX - T_PRODUITS_CHARGEE.PRIX_VNT ) ) ) AS la_somme_Remise_CA
            FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_DATE_CHARGEMENT}'
            GROUP BY 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT
        '''
        
        try:
            kwargs = {
                'Param_DATE_CHARGEMENT': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_DATE_CHARGEMENT'] = self.validateDate(kwargs['Param_DATE_CHARGEMENT'])

        if kwargs['Param_DATE_CHARGEMENT'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_retour_secteur(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_LIVREES.code_secteur AS code_secteur,	
                T_PRODUITS_LIVREES.DATE_VALIDATION AS DATE_VALIDATION,	
                T_PRODUITS_LIVREES.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_LIVREES.TYPE_MVT AS TYPE_MVT,	
                SUM(T_PRODUITS_LIVREES.QTE_CHARGEE) AS la_somme_QTE_CHARGEE
            FROM 
                T_LIVRAISON,	
                T_PRODUITS_LIVREES
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_PRODUITS_LIVREES.NUM_LIVRAISON
                AND
                (
                    T_PRODUITS_LIVREES.TYPE_MVT = 'R'
                    AND	T_PRODUITS_LIVREES.code_secteur = {Param_code_secteur}
                    AND	T_PRODUITS_LIVREES.DATE_VALIDATION = '{Param_date_validation}'
                    AND	T_LIVRAISON.STATUT <> 'A'
                )
            GROUP BY 
                T_PRODUITS_LIVREES.code_secteur,	
                T_PRODUITS_LIVREES.CODE_ARTICLE,	
                T_PRODUITS_LIVREES.TYPE_MVT,	
                T_PRODUITS_LIVREES.DATE_VALIDATION
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_validation': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_solde_autorise(self, args): #Done
        query = '''
            SELECT 
                T_AUTORISATIONS_SOLDE.CODE_OPERATEUR AS CODE_OPERATEUR,	
                SUM(T_AUTORISATIONS_SOLDE.MONTANT) AS la_somme_MONTANT
            FROM 
                T_AUTORISATIONS_SOLDE
            WHERE 
                T_AUTORISATIONS_SOLDE.DATE_OPERATION <= '{Param_dt}'
                AND	T_AUTORISATIONS_SOLDE.DATE_ECHU >= '{Param_dt}'
            GROUP BY 
                T_AUTORISATIONS_SOLDE.CODE_OPERATEUR
        '''

        try:
            kwargs = {
                'Param_dt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_solde_init_clients(self, args): #Done
        query = '''
            SELECT 
                T_SOLDE_INITIAL_CLIENT.DATE_JOURNEE AS DATE_JOURNEE,	
                SUM(T_SOLDE_INITIAL_CLIENT.SOLDE_C_STD) AS la_somme_SOLDE_C_STD,	
                SUM(T_SOLDE_INITIAL_CLIENT.SOLDE_P_AG) AS la_somme_SOLDE_P_AG,	
                SUM(T_SOLDE_INITIAL_CLIENT.SOLDE_P_UHT) AS la_somme_SOLDE_P_UHT,	
                SUM(T_SOLDE_INITIAL_CLIENT.SOLDE_C_AG) AS la_somme_SOLDE_C_AG,	
                SUM(T_SOLDE_INITIAL_CLIENT.SOLDE_C_PR) AS la_somme_SOLDE_C_PR,	
                SUM(T_SOLDE_INITIAL_CLIENT.SOLDE_P_EURO) AS la_somme_SOLDE_P_EURO,	
                SUM(T_SOLDE_INITIAL_CLIENT.SOLDE_C_BLC) AS la_somme_SOLDE_C_BLC,	
                SUM(T_SOLDE_INITIAL_CLIENT.SOLDE_CS1) AS la_somme_SOLDE_CS1,	
                SUM(T_SOLDE_INITIAL_CLIENT.SOLDE_CS2) AS la_somme_SOLDE_CS2
            FROM 
                T_SOLDE_INITIAL_CLIENT
            WHERE 
                T_SOLDE_INITIAL_CLIENT.DATE_JOURNEE = '{Param_date_journee}'
            GROUP BY 
                T_SOLDE_INITIAL_CLIENT.DATE_JOURNEE
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_solde_initial(self, args): #Done
        query = '''
            SELECT 
                T_SOLDE_INITIAL.DATE_JOURNEE AS DATE_JOURNEE,	
                SUM(T_SOLDE_INITIAL.SOLDE_PRODUITS) AS la_somme_SOLDE_PRODUITS,	
                T_OPERATEUR.FONCTION AS FONCTION,	
                SUM(T_SOLDE_INITIAL.MT_ECART) AS la_somme_MT_ECART,	
                SUM(T_SOLDE_INITIAL.MT_A_VERSER) AS la_somme_MT_A_VERSER,	
                SUM(T_SOLDE_INITIAL.MT_VERSER) AS la_somme_MT_VERSER,	
                SUM(T_SOLDE_INITIAL.MT_CHEQUES) AS la_somme_MT_CHEQUES
            FROM 
                T_OPERATEUR,	
                T_SOLDE_INITIAL
            WHERE 
                T_OPERATEUR.CODE_OPERATEUR = T_SOLDE_INITIAL.CODE_OPERATEUR
                AND
                (
                    T_SOLDE_INITIAL.DATE_JOURNEE = '{Param_date_journee}'
                )
            GROUP BY 
                T_SOLDE_INITIAL.DATE_JOURNEE,	
                T_OPERATEUR.FONCTION
        '''
        
        try:
            kwargs = {
                'Param_date_journee': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        if kwargs['Param_date_journee'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_sortie_rendus(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT
            FROM 
                T_MOUVEMENTS,	
                T_OPERATIONS
            WHERE 
                T_OPERATIONS.CODE_OPERATION = T_MOUVEMENTS.ORIGINE
                AND
                (
                    T_MOUVEMENTS.TYPE_MOUVEMENT = 'T'
                    AND	T_OPERATIONS.SOUS_TYPE_OPERATION = 'V'
                    AND	T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                    AND	T_MOUVEMENTS.QTE_MOUVEMENT < 0
                    AND	T_MOUVEMENTS.TYPE_PRODUIT <> 'PRODUIT'
                )
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT
        '''
        
        try:
            kwargs = {
                'Param_date_mvt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_statistiques(self, args): #Done
        query = '''
            SELECT 
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                SUM(T_STATISTIQUES.VENTE_N1) AS la_somme_VENTE_N1,	
                SUM(T_STATISTIQUES.VENTE_CAC) AS la_somme_VENTE_CAC,	
                SUM(T_STATISTIQUES.VENTE) AS la_somme_VENTE
            FROM 
                T_ARTICLES,	
                T_STATISTIQUES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_STATISTIQUES.CODE_ARTICLE
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_STATISTIQUES.DATE_JOURNEE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_ARTICLES.CODE_PRODUIT
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in ('Param_dt1', 'Param_dt2'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_STATISTIQUES.code_secteur = {Param_code_secteur} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_statistiques_canal(self, args): #Done
        query = '''
            SELECT 
                T_STATISTIQUES.CATEGORIE AS CATEGORIE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                SUM(T_STATISTIQUES.VENTE_N1) AS la_somme_VENTE_N1,	
                SUM(T_STATISTIQUES.VENTE_CAC) AS la_somme_VENTE_CAC,	
                SUM(T_STATISTIQUES.VENTE) AS la_somme_VENTE
            FROM 
                T_ARTICLES,	
                T_STATISTIQUES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_STATISTIQUES.CODE_ARTICLE
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_STATISTIQUES.DATE_JOURNEE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_ARTICLES.CODE_PRODUIT,	
                T_STATISTIQUES.CATEGORIE
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_dt1': args[1],
                'Param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in ('Param_dt1', 'Param_dt2'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_STATISTIQUES.code_secteur = {Param_code_secteur} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_secteur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_total_statistiques_client(self, args): #Done
        query = '''
            SELECT 
                T_STATISTIQUES.CODE_CLIENT AS CODE_CLIENT,	
                T_STATISTIQUES.CATEGORIE AS CATEGORIE,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                SUM(T_STATISTIQUES.VENTE_N1) AS la_somme_VENTE_N1,	
                SUM(T_STATISTIQUES.VENTE_CAC) AS la_somme_VENTE_CAC,	
                SUM(T_STATISTIQUES.VENTE) AS la_somme_VENTE
            FROM 
                T_ARTICLES,	
                T_STATISTIQUES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_STATISTIQUES.CODE_ARTICLE
                AND
                (
                    T_STATISTIQUES.DATE_JOURNEE BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                    AND	T_STATISTIQUES.CODE_CLIENT = {Param_code_client}
                )
            GROUP BY 
                T_ARTICLES.CODE_PRODUIT,	
                T_STATISTIQUES.CATEGORIE,	
                T_STATISTIQUES.CODE_CLIENT
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1],
                'Param_code_client': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'])
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_total_susp_operateur(self, args): #Done
        query = '''
            SELECT 
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION AS DATE_VALIDATION,	
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR AS CODE_OPERATEUR,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_STD + T_DT_PRELEVEMENT_COND.DECON_STD + T_DT_PRELEVEMENT_COND.PRIME_STD) AS DR_STD,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_AGR + T_DT_PRELEVEMENT_COND.DECON_AGR + T_DT_PRELEVEMENT_COND.PRIME_AGR) AS DR_AGR,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_PRM + T_DT_PRELEVEMENT_COND.DECON_PRM + T_DT_PRELEVEMENT_COND.PRIME_PRM) AS DR_PRM,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_PAG + T_DT_PRELEVEMENT_COND.DECON_PAG + T_DT_PRELEVEMENT_COND.PRIME_PAG) AS DR_PAG,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_PUHT + T_DT_PRELEVEMENT_COND.DECON_PUHT + T_DT_PRELEVEMENT_COND.PRIME_PUHT) AS DR_PUHT
            FROM 
                T_PRELEVEMENT_SUSP_COND,	
                T_DT_PRELEVEMENT_COND
            WHERE 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV = T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT
                AND
                (
                    T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION <> '1900-01-01 00:00:00'
                )
            GROUP BY 
                T_PRELEVEMENT_SUSP_COND.DATE_VALIDATION,	
                T_DT_PRELEVEMENT_COND.CODE_OPERATEUR
        '''
        return query

    
    def Req_total_transfert_entree(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT,	
                T_OPERATIONS.SOUS_TYPE_OPERATION AS SOUS_TYPE_OPERATION
            FROM 
                T_OPERATIONS,	
                T_MOUVEMENTS
            WHERE 
                T_OPERATIONS.CODE_OPERATION = T_MOUVEMENTS.ORIGINE
                AND
                (
                    T_MOUVEMENTS.QTE_MOUVEMENT > 0
                    AND	T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                    AND	T_MOUVEMENTS.TYPE_MOUVEMENT = 'T'
                    AND	T_MOUVEMENTS.TYPE_PRODUIT = 'PRODUIT'
                    AND	T_OPERATIONS.SOUS_TYPE_OPERATION <> {Param_sous_type}
                )
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT,	
                T_OPERATIONS.SOUS_TYPE_OPERATION
        '''

        try:
            kwargs = {
                'Param_date_mvt': args[0],
                'Param_sous_type': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
            
        return query.format(**kwargs)

    
    def Req_total_transfert_produit_entre_mags(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT,	
                T_MOUVEMENTS.CODE_MAGASIN AS CODE_MAGASIN
            FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.TYPE_MOUVEMENT = 'T'
                AND	T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                AND	T_MOUVEMENTS.TYPE_PRODUIT = 'PRODUIT'
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT,	
                T_MOUVEMENTS.CODE_MAGASIN
        '''
        
        try:
            kwargs = {
                'Param_date_mvt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_transfert_rendus(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT
            FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.TYPE_MOUVEMENT = 'T'
                AND	T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                AND	T_MOUVEMENTS.QTE_MOUVEMENT > 0
                AND	T_MOUVEMENTS.TYPE_PRODUIT <> 'PRODUIT'
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT
        '''
        
        try:
            kwargs = {
                'Param_date_mvt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_transfert_sortie(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT,	
                T_OPERATIONS.CODE_MAGASIN2 AS CODE_MAGASIN2
            FROM 
                T_OPERATIONS,	
                T_MOUVEMENTS
            WHERE 
                T_OPERATIONS.CODE_OPERATION = T_MOUVEMENTS.ORIGINE
                AND
                (
                    T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                    AND	T_MOUVEMENTS.TYPE_MOUVEMENT = 'T'
                    AND	T_MOUVEMENTS.TYPE_PRODUIT = 'PRODUIT'
                    AND	T_OPERATIONS.CODE_MAGASIN2 = 0
                    AND	T_MOUVEMENTS.QTE_MOUVEMENT < 0
                )
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT,	
                T_OPERATIONS.CODE_MAGASIN2
        '''
        
        try:
            kwargs = {
                'Param_date_mvt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_total_transferts_produit(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT
            FROM 
                T_OPERATIONS,	
                T_MOUVEMENTS
            WHERE 
                T_OPERATIONS.CODE_OPERATION = T_MOUVEMENTS.ORIGINE
                AND
                (
                    T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                    AND	T_MOUVEMENTS.TYPE_MOUVEMENT = 'T'
                    AND	T_MOUVEMENTS.TYPE_PRODUIT = 'PRODUIT'
                    AND	T_OPERATIONS.CODE_MAGASIN2 = {Param_code_mag2}
                )
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT
        '''

        try:
            kwargs = {
                'Param_date_mvt': args[0],
                'Param_code_mag2': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_tournee_chargement(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE AS CODE_TOURNEE,	
                T_TOURNEES.NOM_TOURNEE AS NOM_TOURNEE
            FROM 
                T_CHARGEMENT,	
                T_TOURNEES
            WHERE 
                T_CHARGEMENT.CODE_TOURNEE = T_TOURNEES.CODE_TOURNEE
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                    AND	T_CHARGEMENT.code_secteur = {Param_code_secteur}
                )
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_tournee_secteur(self, args): #Done
        query = '''
            SELECT 
                T_TOURNEES.CODE_TOURNEE AS CODE_TOURNEE,	
                T_TOURNEES.code_secteur AS code_secteur,	
                T_SECTEUR.ACTIF AS ACTIF
            FROM 
                T_SECTEUR,	
                T_TOURNEES
            WHERE 
                T_SECTEUR.code_secteur = T_TOURNEES.code_secteur
                AND
                (
                    T_SECTEUR.ACTIF = 1
                    AND	T_TOURNEES.code_secteur = {Param_code_secteur}
                )
        '''
        
        try:
            kwargs = {
                'Param_code_secteur': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_secteur'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_transfert_rendus_par_categorie(self, args): #Done
        query = '''
            SELECT 
                T_MOUVEMENTS.DATE_MVT AS DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT AS TYPE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT AS TYPE_PRODUIT,	
                T_MOUVEMENTS.CODE_ARTICLE AS CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT AS QTE_MOUVEMENT,	
                SUM(T_MOUVEMENTS.QTE_MOUVEMENT) AS la_somme_QTE_MOUVEMENT
            FROM 
                T_MOUVEMENTS
            WHERE 
                T_MOUVEMENTS.TYPE_MOUVEMENT = 'T'
                AND	T_MOUVEMENTS.DATE_MVT = '{Param_date_mvt}'
                AND	T_MOUVEMENTS.QTE_MOUVEMENT > 0
                AND	T_MOUVEMENTS.TYPE_PRODUIT <> 'PRODUIT'
            GROUP BY 
                T_MOUVEMENTS.DATE_MVT,	
                T_MOUVEMENTS.TYPE_MOUVEMENT,	
                T_MOUVEMENTS.TYPE_PRODUIT,	
                T_MOUVEMENTS.CODE_ARTICLE,	
                T_MOUVEMENTS.QTE_MOUVEMENT
        '''
        
        try:
            kwargs = {
                'Param_date_mvt': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_mvt'] = self.validateDate(kwargs['Param_date_mvt'])

        if kwargs['Param_date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_type_produit_mag(self, args): #Done
        query = '''
            SELECT 
                T_TYPE_PRODUIT_MAG.CODE_MAGASIN AS CODE_MAGASIN,	
                T_TYPE_PRODUIT_MAG.PRODUIT AS PRODUIT
            FROM 
                T_TYPE_PRODUIT_MAG
            WHERE 
                T_TYPE_PRODUIT_MAG.CODE_MAGASIN = {Param_code_magasin}
        '''
        
        try:
            kwargs = {
                'Param_code_magasin': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_code_magasin'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_upd_chargement_1(self, args): #Done
        query = '''
            UPDATE 
                T_CHARGEMENT
            SET
                VALID = {Param_VALID},	
                MONTANT_A_VERSER = {Param_MONTANT_A_VERSER}
                {OPTIONAL_ARG_1}
            WHERE 
                T_CHARGEMENT.CODE_CHARGEMENT = {Param_CODE_CHARGEMENT}
        '''

        try:
            kwargs = {
                'Param_VALID': args[0],
                'Param_MONTANT_A_VERSER': args[1],
                'Param_CODE_PREVENDEUR': args[2],
                'Param_CODE_CHARGEMENT': args[3]
            }
        except IndexError as e:
            return e
        
        for key in ('Param_VALID', 'Param_MONTANT_A_VERSER', 'Param_CODE_CHARGEMENT'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = ', CODE_PREVENDEUR = {Param_CODE_PREVENDEUR}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_CODE_PREVENDEUR'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        
        return query.format(**kwargs).format(**kwargs)

    
    def Req_upd_etat_liv(self, args): #Done
        query = '''
            UPDATE 
                T_LIVRAISON
            SET
                MOTIF_ENVOI = {Param_motif}
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = {Param_num_livraison}
        '''

        try:
            kwargs = {
                'Param_motif': args[0],
                'Param_num_livraison': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_upd_maj_secteur(self, args): #Done
        query = '''
            UPDATE 
                T_SECTEUR
            SET
                DERNIER_MAJ = {Param_dernier_maj}
            WHERE 
                T_SECTEUR.code_secteur = {Param_cde_secteur}
        '''

        try:
            kwargs = {
                'Param_dernier_maj': args[0],
                'Param_cde_secteur': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_upd_num_commande(self, args): #Done
        query = '''
            UPDATE 
                T_COMMANDES
            SET
                NUM_COMMANDE = {Param_NUM_COMMANDE}
            WHERE 
                T_COMMANDES.ID_COMMANDE = {Param_ID_COMMANDE}
        '''

        try:
            kwargs = {
                'Param_NUM_COMMANDE': args[0],
                'Param_ID_COMMANDE': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_upd_remise_lait(self, args): #Done
        query = '''
            UPDATE 
                T_REMISE_CLIENT
            SET
                CA_MOY_LAIT = {Param_CA_MOY_LAIT},	
                MT_REMISE_LAIT = {Param_MT_REMISE_LAIT},	
                TX_LAIT = {Param_TX_LAIT}
            WHERE 
                T_REMISE_CLIENT.Date_Debut = '{Param_DATE_DEBUT}'
                AND	T_REMISE_CLIENT.CODE_CLIENT = {Param_CODE_CLIENT}
        '''

        try:
            kwargs = {
                'Param_CA_MOY_LAIT': args[0],
                'Param_MT_REMISE_LAIT': args[1],
                'Param_TX_LAIT': args[2],
                'Param_DATE_DEBUT': args[3],
                'Param_CODE_CLIENT': args[4]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_DATE_DEBUT'] = self.validateDate(kwargs['Param_DATE_DEBUT'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_upd_statut_remise(self, args): #Done
        query = '''
            UPDATE 
                T_REMISE_CLIENT
            SET
                STATUT = {Param_STATUT}
            WHERE 
                T_REMISE_CLIENT.Date_Debut = '{Param_DATE_DEBUT}'
                AND	T_REMISE_CLIENT.STATUT = {Param_STATUT_ACT}
        '''

        try:
            kwargs = {
                'Param_STATUT': args[0],
                'Param_DATE_DEBUT': args[1],
                'Param_STATUT_ACT': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_DATE_DEBUT'] = self.validateDate(kwargs['Param_DATE_DEBUT'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_upd_statut_remise_client(self, args): #Done
        query = '''
            UPDATE 
                T_REMISE_CLIENT
            SET
                STATUT = {Param_STATUT}
            WHERE 
                T_REMISE_CLIENT.Date_Debut = '{Param_DATE_DEBUT}'
                AND	T_REMISE_CLIENT.CODE_CLIENT = {Param_CODE_CLIENT}
                AND	T_REMISE_CLIENT.STATUT = {Param_STATUT_ACT}
        '''

        try:
            kwargs = {
                'Param_STATUT': args[0],
                'Param_DATE_DEBUT': args[1],
                'Param_CODE_CLIENT': args[2],
                'Param_STATUT_ACT': args[3]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_DATE_DEBUT'] = self.validateDate(kwargs['Param_DATE_DEBUT'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_affectation_bl(self, args): #Done
        query = '''
            UPDATE 
                T_LIVRAISON
            SET
                code_vendeur = {Param_code_vendeur}
            WHERE 
                T_LIVRAISON.DATE_LIVRAISON = '{Param_date_livraison}'
                AND	T_LIVRAISON.code_secteur = {Param_code_secteur}
        '''

        try:
            kwargs = {
                'Param_code_vendeur': args[0],
                'Param_date_livraison': args[1],
                'Param_code_secteur': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_affectation_logistique(self, args): #Done
        query = '''
            UPDATE 
                T_CHARGEMENT
            SET
                Id_Vehicule = {ParamID_VEHICULE},	
                ID_VEHICULE_PV = {ParamID_VEHICULE_PV},	
                ID_TRANSPORTEUR = {id_proprietaire}
            WHERE 
                T_CHARGEMENT.DATE_CHARGEMENT = '{ParamDATE_CHARGEMENT}'
                AND	T_CHARGEMENT.code_secteur = {ParamCODE_SECTEUR}
        '''

        try:
            kwargs = {
                'ParamID_VEHICULE': args[0],
                'ParamID_VEHICULE_PV': args[1],
                'id_proprietaire': args[2],
                'ParamDATE_CHARGEMENT': args[3],
                'ParamCODE_SECTEUR': args[4]
            }
        except IndexError as e:
            return e
        
        kwargs['ParamDATE_CHARGEMENT'] = self.validateDate(kwargs['ParamDATE_CHARGEMENT'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_cloture2(self, args): #Done
        query = '''
            UPDATE 
                T_JOURNEE
            SET
                CLOTURE = {Param_CLOTURE}
            WHERE 
                T_JOURNEE.DATE_JOURNEE = '{Param_DATE_JOURNEE}'
        '''

        try:
            kwargs = {
                'Param_CLOTURE': args[0],
                'Param_DATE_JOURNEE': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_decompte'] = self.validateDate(kwargs['Param_date_decompte'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_commande(self, args): #Done
        query = '''
            UPDATE 
                T_COMMANDES
            SET
                TYPE_COMMANDE = {Param_type_commande},	
                DATE_LIVRAISON = '{Param_date_livraison}',	
                code_secteur = {Param_code_secteur},	
                CODE_CLIENT = {Param_code_client},	
                NUM_COMMANDE = {Param_num_commande},	
                OS = {ParamOS}
            WHERE 
                T_COMMANDES.ID_COMMANDE = {Param_id_commande}
        '''

        try:
            kwargs = {
                'Param_type_commande': args[0],
                'Param_date_livraison': args[1],
                'Param_code_secteur': args[2],
                'Param_code_client': args[3],
                'Param_num_commande': args[4],
                'ParamOS': args[5],
                'Param_id_commande': args[6]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_cond(self, args): #Done
        query = '''
            UPDATE 
                T_MAGASIN_COND
            SET
                QTE_STOCK = {Param_qte}
            WHERE 
                {OPTIONAL_ARG_1}
                T_MAGASIN_COND.CODE_CP = {Param_code_cp}
        '''

        try:
            kwargs = {
                'Param_qte': args[0],
                'Param_magasin': args[1],
                'Param_code_cp': args[2]
            }
        except IndexError as e:
            return e
        
        for key in ('Param_qte', 'Param_code_cp'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_MAGASIN_COND.CODE_MAGASIN = {Param_magasin} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_magasin'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_update_dispo(self, args): #Done
        query = '''
            UPDATE 
                T_ARTICLES
            SET
                DISPO = {Param_dispo}
            {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'Param_dispo': args[0],
                'Param_code_article': args[1]
            }
        except IndexError as e:
            return e
        
        if kwargs['Param_dispo'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'WHERE T_ARTICLES.CODE_ARTICLE = {Param_code_article}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_article'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_update_info_trajet(self, args): #Done
        query = '''
            UPDATE 
                T_CHARGEMENT
            SET
                HEURE_SORTIE = '{Param_dateheure_sortie}',	
                HEURE_ENTREE = '{Param_dateheure_entree}',	
                KM_PARCOURUS = {Param_km_parcourus},	
                HEURE_ENTREE_EXEMP = '{Param_heure_exemp}'
            WHERE 
                T_CHARGEMENT.CODE_CHARGEMENT = {Param_code_chargement}
        '''

        try:
            kwargs = {
                'Param_dateheure_sortie': args[0],
                'Param_dateheure_entree': args[1],
                'Param_km_parcourus': args[2],
                'Param_heure_exemp': args[3],
                'Param_code_chargement': args[4]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dateheure_sortie'] = self.validateDate(kwargs['Param_dateheure_sortie'])
        kwargs['Param_dateheure_entree'] = self.validateDate(kwargs['Param_dateheure_entree'])
        kwargs['Param_heure_exemp'] = self.validateDate(kwargs['Param_heure_exemp'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_ligne_commande(self, args): #Done
        query = '''
            UPDATE 
                T_LIGNE_COMMANDE
            SET
                QTE_LIVREE = {Param_qte_livree},	
                QTE_PROMO = {Param_qte_promo},	
                PRIX = {Param_prix},	
                TX_GRATUIT = {Param_tx_gratuit}
            WHERE 
                T_LIGNE_COMMANDE.ID_COMMANDE = {Param_id_commande}
                AND	T_LIGNE_COMMANDE.CODE_ARTICLE = {Param_code_article}
        '''

        try:
            kwargs = {
                'Param_qte_livree': args[0],
                'Param_qte_promo': args[1],
                'Param_prix': args[2],
                'Param_tx_gratuit': args[3],
                'Param_id_commande': args[4],
                'Param_code_article': args[5]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_motif_non_commande(self, args): #Done
        query = '''
            UPDATE 
                T_SYNTHESE_LIVRAISON
            SET
                MOTIF_NON_COMMANDE = {Param_MOTIF_NON_COMMANDE}
            WHERE 
                T_SYNTHESE_LIVRAISON.DATE_JOURNEE = '{Param_DATE_JOURNEE}'
                AND	T_SYNTHESE_LIVRAISON.CODE_CLIENT = {Param_CODE_CLIENT}
        '''

        try:
            kwargs = {
                'Param_MOTIF_NON_COMMANDE': args[0],
                'Param_DATE_JOURNEE': args[1],
                'Param_CODE_CLIENT': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_DATE_JOURNEE'] = self.validateDate(kwargs['Param_DATE_JOURNEE'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_motif_non_livraison(self, args): #Done
        query = '''
            UPDATE 
                T_LIVRAISON
            SET
                MOTIF_NON_VALIDATION = {Param_MOTIF_NON_VALIDATION}
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = {Param_NUM_LIVRAISON}
        '''

        try:
            kwargs = {
                'Param_MOTIF_NON_VALIDATION': args[0],
                'Param_NUM_LIVRAISON': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_NS_CAISSERIE(self, args): #Done
        query = '''
            UPDATE 
                T_JOURNEE
            SET
                SOLDE_EMB = {Param_solde_emballage},	
                NS_C_STD = {Param_NS_STD},	
                NS_P_AG = {Param_NS_P_AG},	
                NS_P_UHT = {Param_NS_P_UHT},	
                NS_C_AG = {Param_NS_C_AG},	
                NS_C_PR = {Param_NS_C_PR},	
                NS_PAL_EURO = {Param_NS_EURO},	
                NS_CS_BLC = {Param_NS_C_BLC},	
                NV_CS1 = {Param_NV_CS1}
            WHERE 
                T_JOURNEE.DATE_JOURNEE = '{Param_date_journee}'
        '''

        try:
            kwargs = {
                'Param_solde_emballage': args[0],
                'Param_NS_STD': args[1],
                'Param_NS_P_AG': args[2],
                'NS_P_UHT': args[3],
                'NS_C_AG': args[4],
                'NS_C_PR': args[5],
                'NS_PAL_EURO': args[6],
                'NS_CS_BLC': args[7],
                'NV_CS1': args[8],
                'Param_date_journee': args[9]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_operateur_versement(self, args): #Done
        query = '''
            UPDATE 
                T_DECOMPTE
            SET
                CODE_OPERATEUR = {Param_nouv_operateur}
            WHERE 
                T_DECOMPTE.CODE_OPERATEUR = {Param_operateur}
                AND	T_DECOMPTE.DATE_DECOMPTE = '{Param_date_decompte}'
                AND	T_DECOMPTE.MODE_PAIEMENT = 'E'
                AND	T_DECOMPTE.REGLEMENT = 0
        '''

        try:
            kwargs = {
                'Param_nouv_operateur': args[0],
                'Param_operateur': args[1],
                'Param_date_decompte': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_decompte'] = self.validateDate(kwargs['Param_date_decompte'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_prix(self, args): #Done
        query = '''
            UPDATE 
                T_PRIX_AGENCE
            SET
                PRIX_VENTE = {Param_prix}
            WHERE 
                T_PRIX_AGENCE.CODE_AGCE = {Param_code_agce}
                AND	T_PRIX_AGENCE.CODE_ARTICLE = {Param_code_article}
        '''

        try:
            kwargs = {
                'Param_prix': args[0],
                'Param_code_agce': args[1],
                'Param_code_article': args[2]
            }
        except IndexError as e:
            return e

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_tx_couverture_article(self, args): #Done
        query = '''
            UPDATE 
                T_ARTICLES
            SET
                TX_COUVERTURE = {Param_tx_couverture}
            WHERE 
                T_ARTICLES.CODE_ARTICLE = {Param_code_article}
        '''

        try:
            kwargs = {
                'Param_tx_couverture': args[0],
                'Param_code_article': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
    
        return query.format(**kwargs)

    
    def Req_update_vendeur_chargement(self, args): #Done
        query = '''
            UPDATE 
                T_PRODUITS_CHARGEE
            SET
                code_vendeur = {Param_code_vendeur}
            WHERE 
                T_PRODUITS_CHARGEE.CODE_CHARGEMENT = {Param_code_chargement}
        '''

        try:
            kwargs = {
                'Param_code_vendeur': args[0],
                'Param_code_chargement': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_update_vendeur_cond(self, args): #Done
        query = '''
            UPDATE 
                T_COND_CHARGEE
            SET
                CODE_OPERATEUR = {Param_code_vendeur}
            WHERE 
                T_COND_CHARGEE.CODE_CHARGEMENT = {Param_code_chargement}
        '''

        try:
            kwargs = {
                'Param_code_vendeur': args[0],
                'Param_code_chargement': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
    
        return query.format(**kwargs)

    
    def Req_val_livraison(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.NUM_LIVRAISON AS NUM_LIVRAISON,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.DATE_LIVRAISON AS DATE_LIVRAISON
            FROM 
                T_LIVRAISON
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = {Param_num_livraison}
        '''
        
        try:
            kwargs = {
                'Param_num_livraison': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_num_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_val_objectif_secteur(self, args):
        query = '''
            SELECT 
                T_OBJECTIF_SECTEURS.DATE_OBJECTIF AS DATE_OBJECTIF,	
                T_OBJECTIF_SECTEURS.code_secteur AS code_secteur,	
                SUM(( T_OBJECTIF_SECTEURS.QTE_OBJECTIF * T_PRIX.PRIX ) ) AS VALEUR_OBJECTIF
            FROM 
                T_PRIX,	
                T_OBJECTIF_SECTEURS,	
                T_ARTICLES,	
                T_PRODUITS
            WHERE 
                T_OBJECTIF_SECTEURS.CODE_PRODUIT = T_PRIX.CODE_ARTICLE
                AND		T_ARTICLES.CODE_PRODUIT = T_PRODUITS.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_OBJECTIF_SECTEURS.CODE_PRODUIT
                AND
                (
                    T_PRIX.Date_Debut <= {Param_dt}
                    AND	T_PRIX.Date_Fin >= {Param_dt}
                    AND	T_OBJECTIF_SECTEURS.DATE_OBJECTIF = {Param_dt}
                    AND	T_OBJECTIF_SECTEURS.code_secteur = {Param_code_secteur}
                    AND	T_PRODUITS.CODE_PRODUIT = {Param_code_produit}
                    AND	T_PRODUITS.CODE_FAMILLE = {Param_code_famille}
                    AND	T_PRODUITS.CAT_PRODUIT = {Param_cat_produit}
                )
            GROUP BY 
                T_OBJECTIF_SECTEURS.DATE_OBJECTIF,	
                T_OBJECTIF_SECTEURS.code_secteur
        '''
        return query.format(**kwargs)

    
    def req_valeur_chargement_secteur(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.code_secteur AS code_secteur,	
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                SUM(( T_PRODUITS_CHARGEE.QTE_CHARGEE * T_PRODUITS_CHARGEE.PRIX ) ) AS VALEUR_CHARGEE,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_SECTEUR.RANG AS RANG,	
                T_CHARGEMENT.vehicule AS vehicule
            FROM 
                T_CHARGEMENT,	
                T_PRODUITS_CHARGEE,	
                T_SECTEUR,	
                T_BLOC,	
                T_ZONE,	
                T_PRODUITS,	
                T_ARTICLES
            WHERE 
                T_ARTICLES.CODE_ARTICLE = T_PRODUITS_CHARGEE.CODE_ARTICLE
                AND		T_ARTICLES.CODE_PRODUIT = T_PRODUITS.CODE_PRODUIT
                AND		T_ZONE.CODE_ZONE = T_BLOC.CODE_ZONE
                AND		T_SECTEUR.CODE_BLOC = T_BLOC.CODE_BLOC
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND		T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND
                (
                    T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}'
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                    {OPTIONAL_ARG_4}
                )
            GROUP BY 
                T_PRODUITS_CHARGEE.code_secteur,	
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT,	
                T_SECTEUR.NOM_SECTEUR,	
                T_SECTEUR.RANG,	
                T_CHARGEMENT.vehicule
            ORDER BY 
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_date_chargement': args[0],
                'Param_code_superviseur': args[1],
                'Param_resp_vente': args[2],
                'Param_code_produit': args[3],
                'Param_code_famille': args[4]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'AND	T_ZONE.CODE_SUPERVISEUR = {Param_code_superviseur}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_superviseur'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_ZONE.RESP_VENTE = {Param_resp_vente}'
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_resp_vente'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = 'AND	T_PRODUITS.CODE_PRODUIT = {Param_code_produit}'
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['Param_code_produit'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']
        kwargs['OPTIONAL_ARG_4'] = 'AND	T_PRODUITS.CODE_FAMILLE = {Param_code_famille}'
        kwargs['OPTIONAL_ARG_4'] = '' if kwargs['Param_code_famille'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_4']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_valeur_chargements(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE AS CODE_TOURNEE,	
                T_CHARGEMENT.code_vendeur AS code_vendeur,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_TOURNEES.NOM_TOURNEE AS NOM_TOURNEE,	
                SUM(T_CHARGEMENT.MONTANT_A_VERSER) AS la_somme_MONTANT_A_VERSER,	
                T_CHARGEMENT.VALID AS VALID,	
                T_OPERATEUR.Matricule AS Matricule
            FROM 
                T_CHARGEMENT,	
                T_TOURNEES,	
                T_OPERATEUR
            WHERE 
                T_CHARGEMENT.code_vendeur = T_OPERATEUR.CODE_OPERATEUR
                AND		T_CHARGEMENT.CODE_TOURNEE = T_TOURNEES.CODE_TOURNEE
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}'
                    AND	T_CHARGEMENT.VALID = 1
                )
            GROUP BY 
                T_CHARGEMENT.DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur,	
                T_CHARGEMENT.CODE_TOURNEE,	
                T_CHARGEMENT.code_vendeur,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_TOURNEES.NOM_TOURNEE,	
                T_CHARGEMENT.VALID,	
                T_OPERATEUR.Matricule
        '''
        
        try:
            kwargs = {
                'Param_date_chargement': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_valeur_commande(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.DATE_LIVRAISON AS DATE_LIVRAISON,	
                T_COMMANDES.TYPE_COMMANDE AS TYPE_COMMANDE,	
                T_COMMANDES.code_secteur AS code_secteur,	
                T_COMMANDES.CODE_CLIENT AS CODE_CLIENT,	
                SUM(( T_PRODUITS_COMMANDES.QTE_U * T_PRIX.PRIX ) ) AS VALEUR_COMMANDE
            FROM 
                T_ARTICLES,	
                T_PRIX,	
                T_PRODUITS_COMMANDES,	
                T_COMMANDES,	
                T_PRODUITS
            WHERE 
                T_COMMANDES.ID_COMMANDE = T_PRODUITS_COMMANDES.ID_COMMANDE
                AND		T_ARTICLES.CODE_PRODUIT = T_PRODUITS.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_COMMANDES.CODE_ARTICLE
                AND		T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND
                (
                    {OPTIONAL_ARG_1}
                    T_COMMANDES.code_secteur = {Param_code_secteur}
                    {OPTIONAL_ARG_2}
                    {OPTIONAL_ARG_3}
                    {OPTIONAL_ARG_4}
                    {OPTIONAL_ARG_5}
                    AND	T_COMMANDES.DATE_LIVRAISON = '{Param_date_livraison}'
                    AND	T_PRIX.Date_Debut <= '{Param_date_livraison}'
                    AND	T_PRIX.Date_Fin >= '{Param_date_livraison}'
                )
            GROUP BY 
                T_COMMANDES.DATE_LIVRAISON,	
                T_COMMANDES.TYPE_COMMANDE,	
                T_COMMANDES.code_secteur,	
                T_COMMANDES.CODE_CLIENT
        '''

        try:
            kwargs = {
                'Param_type_commande': args[0],
                'Param_code_secteur': args[1],
                'Param_code_produit': args[2],
                'Param_code_famille': args[3],
                'Param_cat_produit': args[4],
                'Param_code_client': args[5],
                'Param_date_livraison': args[6]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        for key in ('Param_date_livraison', 'Param_code_secteur'):
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        kwargs['OPTIONAL_ARG_1'] = 'T_COMMANDES.TYPE_COMMANDE = {Param_type_commande} AND'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_type_commande'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_PRODUITS.CODE_PRODUIT = {Param_code_produit}'
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_produit'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['OPTIONAL_ARG_3'] = 'AND	T_PRODUITS.CODE_FAMILLE = {Param_code_famille}'
        kwargs['OPTIONAL_ARG_3'] = '' if kwargs['Param_code_famille'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_3']
        kwargs['OPTIONAL_ARG_4'] = 'AND	T_PRODUITS.CAT_PRODUIT = {Param_cat_produit}'
        kwargs['OPTIONAL_ARG_4'] = '' if kwargs['Param_cat_produit'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_4']
        kwargs['OPTIONAL_ARG_5'] = 'AND	T_COMMANDES.CODE_CLIENT = {Param_code_client}'
        kwargs['OPTIONAL_ARG_5'] = '' if kwargs['Param_code_client'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_5']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_validation_cond_livraison(self, args): #Done
        query = '''
            UPDATE 
                T_COND_LIVRAISON
            SET
                {OPTIONAL_ARG_1}	
                DATE_VALIDATION = '{Param_date_validation}'
            WHERE 
                T_COND_LIVRAISON.NUM_LIVRAISON = {Param_nbl}
        '''

        try:
            kwargs = {
                'Param_qte_chargee': args[0],
                'Param_date_validation': args[1],
                'Param_nbl': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        kwargs['OPTIONAL_ARG_1'] = 'QTE_CHARGEE = {Param_qte_chargee},'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_qte_chargee'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_validation_livraison_prevente(self, args): #Done
        query = '''
            UPDATE 
                T_LIGNE_COMMANDE
            SET 
                QTE_LIVREE = QTE_COMMANDE
            WHERE 
                ID_COMMANDE IN (SELECT ID_COMMANDE FROM T_COMMANDE_CLIENT WHERE DATE_COMMANDE='{param_date}'
                AND code_secteur={param_code_secteur})
            AND 
                CODE_ARTICLE={param_code_article}
        '''

        try:
            kwargs = {
                'param_date': args[0],
                'param_code_secteur': args[1],
                'param_code_article': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['param_date'] = self.validateDate(kwargs['param_date'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
    
        return query.format(**kwargs)

    
    def Req_validation_produits_livraison(self, args): #Done
        query = '''
            UPDATE 
                T_PRODUITS_LIVREES
            SET
                DATE_VALIDATION = '{Param_date_validation}'
            WHERE 
                T_PRODUITS_LIVREES.NUM_LIVRAISON = {Param_nbl}
        '''

        try:
            kwargs = {
                'Param_date_validation': args[0],
                'Param_nbl': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_validation'] = self.validateDate(kwargs['Param_date_validation'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_validation_repartition(self, args): #Done
        query = '''
            UPDATE 
                T_REPARTITION
            SET
                VALIDATION = 1,	
                CONTROLEUR_PRODUIT = {Param_cont_produit},	
                CONTROLEUR_COND = {Param_cont_cond}
            WHERE 
                T_REPARTITION.DATE_REPARTITION = '{Param_date_repartition}'
        '''

        try:
            kwargs = {
                'Param_cont_produit': args[0],
                'Param_cont_cond': args[1],
                'Param_date_repartition': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_repartition'] = self.validateDate(kwargs['Param_date_repartition'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_vent_n1_produit(self, args): #Done
        query = '''
            SELECT 
                T_MOY_VENTE_ARTICLE.DATE_VENTE AS DATE_VENTE,	
                T_MOY_VENTE_ARTICLE.CODE_PRODUIT AS CODE_ARTICLE,	
                T_MOY_VENTE_ARTICLE.code_secteur AS code_secteur,	
                T_MOY_VENTE_ARTICLE.QTE_VENTE AS QTE_VENTE,	
                T_MOY_VENTE_ARTICLE.QTE_PERTE AS QTE_PERTE,	
                T_MOY_VENTE_ARTICLE.QTE_INVENDU AS QTE_INVENDU,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT
            FROM 
                T_ARTICLES,	
                T_MOY_VENTE_ARTICLE,	
                T_PRODUITS
            WHERE 
                T_ARTICLES.CODE_PRODUIT = T_PRODUITS.CODE_PRODUIT
                AND		T_ARTICLES.CODE_ARTICLE = T_MOY_VENTE_ARTICLE.CODE_PRODUIT
                AND
                (
                    T_MOY_VENTE_ARTICLE.DATE_VENTE = '{Param_date}'
                    AND	T_MOY_VENTE_ARTICLE.code_secteur = {Param_code_secteur}
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )
        '''

        try:
            kwargs = {
                'Param_date': args[0],
                'Param_code_secteur': args[1],
                'Param_code_produit': args[2],
                'Param_code_famille': args[3]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_ARTICLES.CODE_PRODUIT = {Param_code_produit}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_produit'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']
        kwargs['OPTIONAL_ARG_2'] = 'AND	T_PRODUITS.CODE_FAMILLE = {Param_code_famille}'
        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_code_famille'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs).format(**kwargs)

    
    def Req_vente_n1_client(self, args): #Done
        query = '''
            SELECT 
                T_MOY_VENTE_CLIENTS.DATE_VENTE AS DATE_VENTE,	
                T_MOY_VENTE_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_MOY_VENTE_CLIENTS.CODE_PRODUIT AS CODE_ARTICLE,	
                T_MOY_VENTE_CLIENTS.QTE_VENTE AS QTE_VENTE,	
                T_MOY_VENTE_CLIENTS.QTE_PERTE AS QTE_PERTE
            FROM 
                T_MOY_VENTE_CLIENTS
            WHERE 
                T_MOY_VENTE_CLIENTS.DATE_VENTE = '{Param_date}'
                AND	T_MOY_VENTE_CLIENTS.CODE_CLIENT = {Param_code_client}
        '''

        try:
            kwargs = {
                'Param_date': args[0],
                'Param_code_client': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date'] = self.validateDate(kwargs['Param_date'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_vente_nette(self, args): #Done
        query = '''
            SELECT 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                SUM(T_PRODUITS_CHARGEE.MONTANT) AS la_somme_MONTANT
            FROM 
                T_PRODUITS_CHARGEE
            WHERE 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT = '{Param_date_chargement}'
            GROUP BY 
                T_PRODUITS_CHARGEE.DATE_CHARGEMENT
        '''
        
        try:
            kwargs = {
                'Param_date_chargement': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])

        if kwargs['Param_date_chargement'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_ventes_secteur(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_ARTICLES.CODE_PRODUIT AS CODE_PRODUIT,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_PRODUITS_CHARGEE.PRIX AS PRIX,	
                T_PRODUITS_CHARGEE.TOTAL_CHARGEE AS TOTAL_CHARGEE,	
                T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE AS TOTAL_INVENDU_POINTE,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM AS TOTAL_RENDUS_COM,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_US AS TOTAL_RENDUS_US,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG AS TOTAL_RENDUS_AG,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP AS TOTAL_RENDUS_SP,	
                T_PRODUITS_CHARGEE.TOTAL_GRATUIT AS TOTAL_GRATUIT,	
                T_PRODUITS_CHARGEE.TOTAL_DONS AS TOTAL_DONS,	
                T_PRODUITS_CHARGEE.TOTAL_REMISE AS TOTAL_REMISE,	
                T_SECTEUR.RANG AS RANG_secteur,	
                T_ARTICLES.RANG AS RANG_article,	
                T_PRODUITS_CHARGEE.QTE_ECART AS QTE_ECART,	
                T_PRODUITS_CHARGEE.TOTAL_VENDU AS TOTAL_VENDU,	
                T_PRODUITS_CHARGEE.CREDIT AS CREDIT,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE AS QTE_CHARGEE_POINTE,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_VAL AS QTE_CHARGEE_VAL,	
                T_PRODUITS_CHARGEE.QTE_CHARGEE_SUPP AS QTE_CHARGEE_SUPP,	
                T_PRODUITS_CHARGEE.MONTANT AS MONTANT,	
                T_PRODUITS_CHARGEE.MONTANT_CREDIT AS MONTANT_CREDIT,	
                T_PRODUITS_CHARGEE.TOTAL_RENDUS_POINTE AS TOTAL_RENDUS_POINTE,	
                T_PRODUITS_CHARGEE.CODE_ARTICLE AS CODE_ARTICLE,	
                T_PRODUITS_CHARGEE.MONTANT_ECART AS MONTANT_ECART
            FROM 
                T_ARTICLES,	
                T_PRODUITS_CHARGEE,	
                T_CHARGEMENT,	
                T_SECTEUR
            WHERE 
                T_CHARGEMENT.code_secteur = T_SECTEUR.code_secteur
                AND		T_PRODUITS_CHARGEE.CODE_CHARGEMENT = T_CHARGEMENT.CODE_CHARGEMENT
                AND		T_PRODUITS_CHARGEE.CODE_ARTICLE = T_ARTICLES.CODE_ARTICLE
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            ORDER BY 
                RANG_secteur ASC,	
                RANG_article ASC
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_ventes_secteur_produit(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                T_PRODUITS_CHARGEE.PRIX AS PRIX,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_CHARGEE) AS la_somme_TOTAL_CHARGEE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE) AS la_somme_TOTAL_INVENDU_POINTE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_COM) AS la_somme_TOTAL_RENDUS_COM,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_US) AS la_somme_TOTAL_RENDUS_US,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_AG) AS la_somme_TOTAL_RENDUS_AG,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_SP) AS la_somme_TOTAL_RENDUS_SP,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE_VAL) AS la_somme_QTE_CHARGEE_VAL,	
                SUM(T_PRODUITS_CHARGEE.MONTANT_ECART) AS la_somme_MONTANT_ECART,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_POINTE) AS la_somme_TOTAL_RENDUS_POINTE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_REMISE) AS la_somme_TOTAL_REMISE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_DONS) AS la_somme_TOTAL_DONS,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_GRATUIT) AS la_somme_TOTAL_GRATUIT,	
                SUM(T_PRODUITS_CHARGEE.MONTANT_CREDIT) AS la_somme_MONTANT_CREDIT,	
                SUM(T_PRODUITS_CHARGEE.CREDIT) AS la_somme_CREDIT,	
                SUM(T_PRODUITS_CHARGEE.MONTANT) AS la_somme_MONTANT,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_VENDU) AS la_somme_TOTAL_VENDU,	
                SUM(T_PRODUITS_CHARGEE.QTE_ECART) AS la_somme_QTE_ECART,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE_SUPP) AS la_somme_QTE_CHARGEE_SUPP,	
                SUM(T_PRODUITS_CHARGEE.QTE_CHARGEE_POINTE) AS la_somme_QTE_CHARGEE_POINTE,	
                T_SECTEUR.RANG AS RANG_SECTEUR,	
                MAX(T_ARTICLES.RANG) AS le_maximum_RANG,	
                MIN(T_PRODUITS_CHARGEE.CODE_ARTICLE) AS le_minimum_CODE_ARTICLE
            FROM 
                T_ARTICLES,	
                T_PRODUITS_CHARGEE,	
                T_CHARGEMENT,	
                T_SECTEUR,	
                T_PRODUITS
            WHERE 
                T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_CHARGEE.CODE_ARTICLE
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_CHARGEMENT.DATE_CHARGEMENT,	
                T_CHARGEMENT.code_secteur,	
                T_SECTEUR.NOM_SECTEUR,	
                T_PRODUITS.NOM_PRODUIT,	
                T_PRODUITS_CHARGEE.PRIX,	
                T_SECTEUR.RANG
            ORDER BY 
                RANG_SECTEUR ASC,	
                le_minimum_CODE_ARTICLE ASC
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Req_verif_chargement(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.code_secteur AS code_secteur,	
                T_CHARGEMENT.HEURE_SORTIE AS HEURE_SORTIE,	
                T_CHARGEMENT.HEURE_ENTREE AS HEURE_ENTREE,	
                T_CHARGEMENT.KM_PARCOURUS AS KM_PARCOURUS,	
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_CHARGEMENT.CODE_CHARGEMENT AS CODE_CHARGEMENT
            FROM 
                T_SECTEUR,	
                T_CHARGEMENT
            WHERE 
                T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                {CODE_BLOCK_1}
        '''

        try:
            kwargs = {
                'Param_nom_secteur': args[0],
                'Param_date_chargement': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_chargement'] = self.validateDate(kwargs['Param_date_chargement'])
        
        kwargs['CODE_BLOCK_1'] = '''AND
                (
                    {OPTIONAL_ARG_1}
                    {OPTIONAL_ARG_2}
                )'''
        
        kwargs['OPTIONAL_ARG_1'] = 'T_SECTEUR.NOM_SECTEUR = {Param_nom_secteur}'
        kwargs['OPTIONAL_ARG_2'] = ''''AND T_CHARGEMENT.DATE_CHARGEMENT = '{Param_date_chargement}' '''

        if kwargs['OPTIONAL_ARG_1'] in (None, 'NULL'):
            kwargs['OPTIONAL_ARG_1'] = ''
            kwargs['OPTIONAL_ARG_2'] = kwargs['OPTIONAL_ARG_2'][4:]

        kwargs['OPTIONAL_ARG_2'] = '' if kwargs['Param_date_chargement'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_2']
        kwargs['CODE_BLOCK_1'] = '' if kwargs['OPTIONAL_ARG_2'] == '' and kwargs['OPTIONAL_ARG_1'] == '' else kwargs['CODE_BLOCK_1']

        return query.format(**kwargs).format(**kwargs).format(**kwargs)

    
    def Req_verif_envoi_perte(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS.CATEGORIE1 AS CATEGORIE1,	
                T_OPERATIONS.SOUS_TYPE_OPERATION AS SOUS_TYPE_OPERATION
            FROM 
                T_OPERATIONS
            WHERE 
                T_OPERATIONS.DATE_OPERATION = '{Param_date_operation}' AND
                T_OPERATIONS.CATEGORIE1 LIKE 'PNC%'
                AND	T_OPERATIONS.SOUS_TYPE_OPERATION = 'V'
        '''

        try:
            kwargs = {
                'Param_date_operation': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_operation'] = self.validateDate(kwargs['Param_date_operation'])
        kwargs['OPTIONAL_ARG_1'] = '''T_OPERATIONS.DATE_OPERATION = '{Param_date_operation}' AND'''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_date_operation'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)

    
    def Req_verif_n1_clients(self, args): #Done
        query = '''
            SELECT 
                T_MOY_VENTE_CLIENTS.DATE_VENTE AS DATE_VENTE,	
                T_MOY_VENTE_CLIENTS.CODE_CLIENT AS CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT AS NOM_CLIENT,	
                SUM(( T_MOY_VENTE_CLIENTS.QTE_VENTE * T_PRIX.PRIX ) ) AS CA_MOY
            FROM 
                T_MOY_VENTE_CLIENTS,	
                T_PRIX,	
                T_CLIENTS
            WHERE 
                T_CLIENTS.CODE_CLIENT = T_MOY_VENTE_CLIENTS.CODE_CLIENT
                AND		T_MOY_VENTE_CLIENTS.CODE_PRODUIT = T_PRIX.CODE_ARTICLE
                AND
                (
                    T_PRIX.Date_Debut <= '{param_dt}'
                    AND	T_PRIX.Date_Fin >= '{param_dt}'
                    AND	T_MOY_VENTE_CLIENTS.DATE_VENTE = '{param_dt}'
                )
            GROUP BY 
                T_MOY_VENTE_CLIENTS.DATE_VENTE,	
                T_MOY_VENTE_CLIENTS.CODE_CLIENT,	
                T_CLIENTS.NOM_CLIENT
        '''

        try:
            kwargs = {
                'param_dt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['param_dt'] = self.validateDate(kwargs['param_dt'])

        if kwargs['param_dt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_verif_n1_secteurs(self, args): #Done
        query = '''
            SELECT 
                T_MOY_VENTE_ARTICLE.DATE_VENTE AS DATE_VENTE,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                SUM(( T_MOY_VENTE_ARTICLE.QTE_VENTE * T_PRIX.PRIX ) ) AS CA_MOY
            FROM 
                T_MOY_VENTE_ARTICLE,	
                T_ARTICLES,	
                T_PRIX,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_MOY_VENTE_ARTICLE.code_secteur
                AND		T_ARTICLES.CODE_ARTICLE = T_PRIX.CODE_ARTICLE
                AND		T_ARTICLES.CODE_ARTICLE = T_MOY_VENTE_ARTICLE.CODE_PRODUIT
                AND
                (
                    T_MOY_VENTE_ARTICLE.DATE_VENTE = '{Param_dt}'
                    AND	T_PRIX.Date_Debut <= '{Param_dt}'
                    AND	T_PRIX.Date_Fin >= '{Param_dt}'
                )
            GROUP BY 
                T_MOY_VENTE_ARTICLE.DATE_VENTE,	
                T_SECTEUR.NOM_SECTEUR
        '''

        try:
            kwargs = {
                'Param_dt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        return query.format(**kwargs)

    
    def Req_verif_nbl(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS.REF AS REF
            FROM 
                T_OPERATIONS
            WHERE 
                T_OPERATIONS.REF = {Param_num_bl}
                AND	T_OPERATIONS.TYPE_OPERATION = 'R'
        '''

        try:
            kwargs = {
                'Param_num_bl': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_num_bl'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_verif_num_commande(self, args): #Done
        query = '''
            SELECT 
                T_COMMANDES.ID_COMMANDE AS ID_COMMANDE,	
                T_COMMANDES.NUM_COMMANDE AS NUM_COMMANDE
            FROM 
                T_COMMANDES
            WHERE 
                T_COMMANDES.NUM_COMMANDE = {Param_num_commande}
        '''

        try:
            kwargs = {
                'Param_num_commande': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_num_commande'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_verif_prelev_caisserie(self, args): #Done
        query = '''
            SELECT 
                T_PRELEVEMENT_SUSP_COND.DATE_PRELEV AS DATE_PRELEV,	
                SUM(T_DT_PRELEVEMENT_COND.SOLDE_STD) AS la_somme_SOLDE_STD
            FROM 
                T_PRELEVEMENT_SUSP_COND,	
                T_DT_PRELEVEMENT_COND
            WHERE 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV = T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT
                AND
                (
                    T_PRELEVEMENT_SUSP_COND.DATE_PRELEV = '{Param_date_prelev}'
                )
            GROUP BY 
                T_PRELEVEMENT_SUSP_COND.DATE_PRELEV
        '''
        
        try:
            kwargs = {
                'Param_date_prelev': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_prelev'] = self.validateDate(kwargs['Param_date_prelev'])

        if kwargs['Param_date_prelev'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_verif_prelevement(self, args): #Done
        query = '''
            SELECT 
                T_PRELEVEMENT_SUSP_COND.DATE_PRELEV AS DATE_PRELEV,	
                SUM(T_DT_PRELEVEMENT_COND.SUSP_VENTE) AS la_somme_SUSP_VENTE
            FROM 
                T_PRELEVEMENT_SUSP_COND,	
                T_DT_PRELEVEMENT_COND
            WHERE 
                T_PRELEVEMENT_SUSP_COND.ID_PRELEV = T_DT_PRELEVEMENT_COND.ID_PRELEVEMENT
                AND
                (
                    T_PRELEVEMENT_SUSP_COND.DATE_PRELEV = '{Param_date_prelev}'
                )
            GROUP BY 
                T_PRELEVEMENT_SUSP_COND.DATE_PRELEV
        '''
        
        try:
            kwargs = {
                'Param_date_prelev': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_prelev'] = self.validateDate(kwargs['Param_date_prelev'])

        if kwargs['Param_date_prelev'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_verif_synchro(self, args): #Done
        query = '''
            SELECT 
                T_SYNCHRO.OPERATION AS OPERATION,	
                T_SYNCHRO.SOUS_OPERATION AS SOUS_OPERATION,	
                T_SYNCHRO.ID_OPERATION AS ID_OPERATION
            FROM 
                T_SYNCHRO
            WHERE 
                T_SYNCHRO.OPERATION = {Param_operation}
                AND	T_SYNCHRO.SOUS_OPERATION = {Param_sous_operation}
                AND	T_SYNCHRO.ID_OPERATION = {Param_id_operation}
        '''

        try:
            kwargs = {
                'Param_operation': args[0],
                'Param_sous_operation': args[1],
                'Param_id_operation': args[2]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_verif_trait_pda(self, args): #Done
        query = '''
            SELECT 
                T_HIST_TRAITEMENT_PDA.code_secteur AS code_secteur,	
                T_HIST_TRAITEMENT_PDA.DATE_JOURNEE AS DATE_JOURNEE,	
                T_HIST_TRAITEMENT_PDA.VALID AS VALID,	
                T_HIST_TRAITEMENT_PDA.MT_A_VERSER AS MT_A_VERSER
            FROM 
                T_HIST_TRAITEMENT_PDA
            WHERE 
                T_HIST_TRAITEMENT_PDA.code_secteur = {Param_code_secteur}
                AND	T_HIST_TRAITEMENT_PDA.DATE_JOURNEE = '{Param_date_journee}'
                AND	T_HIST_TRAITEMENT_PDA.VALID = 1
        '''

        try:
            kwargs = {
                'Param_code_secteur': args[0],
                'Param_date_journee': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)

    
    def Req_verif_traitement(self, args): #Done
        query = '''
            SELECT 
                T_HISTORIQUE_OPERATIONS.COMMENTAIRE AS COMMENTAIRE
            FROM 
                T_HISTORIQUE_OPERATIONS
            WHERE 
                T_HISTORIQUE_OPERATIONS.COMMENTAIRE = '{Param_cmt}'
        '''

        try:
            kwargs = {
                'Param_cmt': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['Param_cmt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_verif_tx_remise(self, args): #Done
        query = '''
            SELECT DISTINCT 
                T_REMISE_CLIENT.Date_Debut AS Date_Debut,	
                T_REMISE_CLIENT.TX_DERIVES AS TX_DERIVES
            FROM 
                T_REMISE_CLIENT
            WHERE 
                T_REMISE_CLIENT.Date_Debut = '{Param_date_debut}'
        '''
        
        try:
            kwargs = {
                'Param_date_debut': args[0]
            }
        except IndexError as e:
            return e

        kwargs['Param_date_debut'] = self.validateDate(kwargs['Param_date_debut'])

        if kwargs['Param_date_debut'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)

    
    def Req_versement_non_envoyer(self, args): #Done
        query = '''
            SELECT 
                T_OPERATIONS_CAISSE.CODE_OPERATION AS CODE_OPERATION,	
                T_OPERATIONS_CAISSE.TYPE_OPERATION AS TYPE_OPERATION,	
                T_OPERATIONS_CAISSE.DATE_OPERATION AS DATE_OPERATION,	
                T_OPERATIONS_CAISSE.NUM_PIECE AS NUM_PIECE,	
                T_OPERATIONS_CAISSE.COMPTE_VERSEMENT AS COMPTE_VERSEMENT,	
                T_OPERATIONS_CAISSE.COMMENTAIRE AS COMMENTAIRE,	
                T_OPERATIONS_CAISSE.MONTANT AS MONTANT,	
                T_COMPTES.NUM_COMPTE AS NUM_COMPTE,	
                T_BANQUES.LIBELLE AS LIBELLE,	
                T_OPERATIONS_CAISSE.DATE_VALIDATION AS DATE_VALIDATION,	
                T_OPERATIONS_CAISSE.MOTIF_ENVOI AS MOTIF_ENVOI
            FROM 
                T_BANQUES,	
                T_COMPTES,	
                T_OPERATIONS_CAISSE
            WHERE 
                T_COMPTES.CODE_COMPTE = T_OPERATIONS_CAISSE.COMPTE_VERSEMENT
                AND		T_BANQUES.NUM_BANQUE = T_COMPTES.BANQUE
                AND
                (
                    T_OPERATIONS_CAISSE.MOTIF_ENVOI <> 1
                )
            ORDER BY 
                CODE_OPERATION DESC
        '''
        return query

    
    def Req_zero_stock(self, args): #Done
        query = '''
            UPDATE 
                T_ARTICLES_MAGASINS
            SET
                QTE_STOCK = 0
        '''
        return query

    
    def Requ_mvt_cond_gms(self, args): #Done
        query = '''
            SELECT 
                T_LIVRAISON.TYPE_MVT AS TYPE_MVT,	
                T_LIVRAISON.DATE_VALIDATION AS DATE_VALIDATION,	
                T_LIVRAISON.STATUT AS STATUT,	
                T_LIVRAISON.CODE_CLIENT AS CODE_CLIENT,	
                T_COND_LIVRAISON.CODE_CP AS CODE_CP,	
                SUM(T_COND_LIVRAISON.QTE_CHARGEE) AS la_somme_QTE_CHARGEE
            FROM 
                T_LIVRAISON,	
                T_COND_LIVRAISON
            WHERE 
                T_LIVRAISON.NUM_LIVRAISON = T_COND_LIVRAISON.NUM_LIVRAISON
                AND
                (
                    T_LIVRAISON.DATE_VALIDATION = '{Param_date_livraison}'
                    AND	T_LIVRAISON.STATUT <> 'A'
                    {OPTIONAL_ARG_1}
                    AND	T_LIVRAISON.TYPE_MVT IN ('l', 'R') 
                )
            GROUP BY 
                T_LIVRAISON.DATE_VALIDATION,	
                T_LIVRAISON.STATUT,	
                T_LIVRAISON.CODE_CLIENT,	
                T_COND_LIVRAISON.CODE_CP,	
                T_LIVRAISON.TYPE_MVT
        '''

        try:
            kwargs = {
                'Param_date_livraison': args[0],
                'Param_code_cp': args[1]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_livraison'] = self.validateDate(kwargs['Param_date_livraison'])

        kwargs['OPTIONAL_ARG_1'] = 'AND	T_COND_LIVRAISON.CODE_CP = {Param_code_cp}'
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['Param_code_cp'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        if kwargs['Param_date_livraison'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs).format(**kwargs)

    
    def Requête1(self, args): #Done
        query = '''
            SELECT 
                T_CHARGEMENT.DATE_CHARGEMENT AS DATE_CHARGEMENT,	
                T_SECTEUR.NOM_SECTEUR AS NOM_SECTEUR,	
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR,	
                T_GAMME.NOM_GAMME AS NOM_GAMME,	
                T_FAMILLE.NOM_FAMILLE AS NOM_FAMILLE,	
                T_PRODUITS.NOM_PRODUIT AS NOM_PRODUIT,	
                T_ARTICLES.LIBELLE_COURT AS LIBELLE_COURT,	
                T_ARTICLES.RANG AS RANG,	
                T_PRODUITS_CHARGEE.PRIX AS PRIX,	
                SUM(T_PRODUITS_CHARGEE.MONTANT) AS la_somme_MONTANT,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_CHARGEE) AS la_somme_TOTAL_CHARGEE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_VENDU) AS la_somme_TOTAL_VENDU,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_INVENDU_POINTE) AS la_somme_TOTAL_INVENDU_POINTE,	
                SUM(T_PRODUITS_CHARGEE.TOTAL_RENDUS_POINTE) AS la_somme_TOTAL_RENDUS_POINTE
            FROM 
                T_GAMME,	
                T_FAMILLE,	
                T_PRODUITS,	
                T_ARTICLES,	
                T_PRODUITS_CHARGEE,	
                T_CHARGEMENT,	
                T_OPERATEUR,	
                T_SECTEUR
            WHERE 
                T_SECTEUR.code_secteur = T_CHARGEMENT.code_secteur
                AND		T_OPERATEUR.CODE_OPERATEUR = T_CHARGEMENT.code_vendeur
                AND		T_CHARGEMENT.CODE_CHARGEMENT = T_PRODUITS_CHARGEE.CODE_CHARGEMENT
                AND		T_ARTICLES.CODE_ARTICLE = T_PRODUITS_CHARGEE.CODE_ARTICLE
                AND		T_PRODUITS.CODE_PRODUIT = T_ARTICLES.CODE_PRODUIT
                AND		T_FAMILLE.CODE_FAMILLE = T_PRODUITS.CODE_FAMILLE
                AND		T_GAMME.CODE_GAMME = T_FAMILLE.CODE_GAMME
                AND
                (
                    T_CHARGEMENT.DATE_CHARGEMENT BETWEEN '{Param_dt1}' AND '{Param_dt2}'
                )
            GROUP BY 
                T_CHARGEMENT.DATE_CHARGEMENT,	
                T_SECTEUR.NOM_SECTEUR,	
                T_OPERATEUR.NOM_OPERATEUR,	
                T_PRODUITS_CHARGEE.PRIX,	
                T_ARTICLES.RANG,	
                T_ARTICLES.LIBELLE_COURT,	
                T_FAMILLE.NOM_FAMILLE,	
                T_GAMME.NOM_GAMME,	
                T_PRODUITS.NOM_PRODUIT
            ORDER BY 
                DATE_CHARGEMENT ASC,	
                NOM_SECTEUR ASC,	
                NOM_GAMME ASC,	
                NOM_FAMILLE ASC,	
                NOM_PRODUIT ASC,	
                RANG ASC
        '''

        try:
            kwargs = {
                'Param_dt1': args[0],
                'Param_dt2': args[1]
            }
        except IndexError as e:
            return e

        kwargs['Param_dt1'] = self.validateDate(kwargs['Param_dt1'], 0)
        kwargs['Param_dt2'] = self.validateDate(kwargs['Param_dt2'], 1)

        return query.format(**kwargs)

    
    def Requête12(self, args): #Done
        query = '''
            SELECT 
                art.CODE_ARTICLE, art.LIBELLE AS nom_article,
                cm.CODE_CHARGEMENT AS cde_chargement, cm.QTE_CHARGEE AS qte_cmd
            FROM
                T_ARTICLES art
            LEFT OUTER JOIN 
                T_PRODUITS_CHARGEE cm
            ON 
                cm.CODE_ARTICLE = art.CODE_ARTICLE

            WHERE 
                cm.CODE_CHARGEMENT = 13000003
        '''
        return query
    

    def journee_1_Requete(self, args): #Done
        query = '''
            SELECT TOP 10 
                T_JOURNEE.DATE_JOURNEE AS DATE_JOURNEE,	
                T_JOURNEE.CODE_AGCE AS CODE_AGCE,	
                T_JOURNEE.STOCK AS STOCK,	
                T_JOURNEE.SOLDE_EMB AS SOLDE_EMB,	
                T_JOURNEE.CLOTURE AS CLOTURE,	
                T_JOURNEE.JOURNEE_TEMP AS JOURNEE_TEMP,	
                T_JOURNEE.SOLDE_CAISSE AS SOLDE_CAISSE,	
                T_JOURNEE.AS_C_STD AS AS_C_STD,	
                T_JOURNEE.AS_P_AG AS AS_P_AG,	
                T_JOURNEE.AS_P_UHT AS AS_P_UHT,	
                T_JOURNEE.AS_C_AG AS AS_C_AG,	
                T_JOURNEE.AS_C_PR AS AS_C_PR,	
                T_JOURNEE.NS_C_STD AS NS_C_STD,	
                T_JOURNEE.NS_P_AG AS NS_P_AG,	
                T_JOURNEE.NS_P_UHT AS NS_P_UHT,	
                T_JOURNEE.NS_C_AG AS NS_C_AG,	
                T_JOURNEE.NS_C_PR AS NS_C_PR,	
                T_JOURNEE.SOLDE_CAISSERIE AS SOLDE_CAISSERIE,	
                T_JOURNEE.TEMP_MIN AS TEMP_MIN,	
                T_JOURNEE.TEMP_MAX AS TEMP_MAX,	
                T_JOURNEE.PLUV AS PLUV,	
                T_JOURNEE.COMMENTAIRE AS COMMENTAIRE,	
                T_JOURNEE.AS_P_EURO AS AS_P_EURO,	
                T_JOURNEE.AS_CS_BLC AS AS_CS_BLC,	
                T_JOURNEE.NS_PAL_EURO AS NS_PAL_EURO,	
                T_JOURNEE.NS_CS_BLC AS NS_CS_BLC,	
                T_JOURNEE.AS_CS1 AS AS_CS1,	
                T_JOURNEE.AS_CS2 AS AS_CS2,	
                T_JOURNEE.NV_CS1 AS NV_CS1,	
                T_JOURNEE.NV_CS2 AS NV_CS2
            FROM 
                T_JOURNEE
            ORDER BY 
                DATE_JOURNEE DESC
        '''
        return query


# --------------- Conversions ----------------------
    def Req_conv_login(self, args): #Done2
        query = '''
            SELECT
                T_OPERATEUR.CODE_OPERATEUR AS CODE_OPERATEUR,
                T_OPERATEUR.MDP AS MDP,
                T_OPERATEUR.FONCTION AS FONCTION
            FROM
                T_OPERATEUR
            {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'code_op': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = '''WHERE
                T_OPERATEUR.CODE_OPERATEUR = {code_op}'''
        
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['code_op'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)
    
    def Req_conv_parametres(self, args): #Done2
        query = '''
            SELECT
                T_PARAMETRES.ID_LIGNE AS ID_LIGNE,
                T_PARAMETRES.VALEUR AS VALEUR
            FROM
                T_PARAMETRES
            {OPTIONAL_ARG_1}
        '''

        try:
            kwargs = {
                'id_ligne': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = '''WHERE
                T_PARAMETRES.ID_LIGNE = {id_ligne}'''
        
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['id_ligne'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)


    def Req_conv_generation_in_1(self, args): #Done2
        query = '''
            SELECT 
                T_ARTICLES.CODE_ARTICLE,
                RANG,
                ABREVIATION,
                LIBELLE_COURT,
                CONDITIONNEMENT,
                QTE_PALETTE,
                TYPE_CAISSE,
                TYPE_PALETTE,
                CODE_PRODUIT,
                QTE_PACK,
                T_PRIX_AGENCE.PRIX_VENTE,
                TVA,
                AFF_REPARTITION,
                DISPO
            FROM 
                T_ARTICLES,
                T_PRIX_AGENCE 
            WHERE 
                {OPTIONAL_ARG_1}
                T_ARTICLES.CODE_ARTICLE=T_PRIX_AGENCE.CODE_ARTICLE
                AND T_ARTICLES.ACTIF='1'
                AND aff_commande='0'
        '''

        try:
            kwargs = {
                'param_code_agce': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['OPTIONAL_ARG_1'] = '''CODE_AGCE='{param_code_agce}' AND'''
        kwargs['OPTIONAL_ARG_1'] = '' if kwargs['param_code_agce'] in (None, 'NULL') else kwargs['OPTIONAL_ARG_1']

        return query.format(**kwargs).format(**kwargs)


    def Req_conv_generation_in_2(self, args): #Done2
        query = '''
            select T_PRODUITS.CODE_PRODUIT,NOM_PRODUIT,NOM_FAMILLE,NOM_GAMME,CYCLE from T_PRODUITS,T_FAMILLE,T_GAMME
            where T_PRODUITS.CODE_FAMILLE = T_FAMILLE.CODE_FAMILLE and T_GAMME.CODE_GAMME = T_FAMILLE.CODE_GAMME
        '''

        return query


    def Req_conv_generation_in_3(self, args): #Done2
        query = '''
            select CODE_SECTEUR,NOM_SECTEUR,CODE_ZONE,(select COUNT(*) from T_TOURNEES where T_TOURNEES.CODE_SECTEUR = T_SECTEUR.CODE_SECTEUR and T_TOURNEES.ACTIF=1)AS NB_TRN from T_SECTEUR,T_BLOC
            where T_SECTEUR.CODE_BLOC = T_BLOC.CODE_BLOC
        '''

        return query


    def Req_conv_generation_in_4(self, args): #Done2
        query = '''
            SELECT CODE_SOUS_SECTEUR,NOM_SOUS_SECTEUR FROM T_SOUS_SECTEUR
        '''

        return query


    def Req_conv_generation_in_5(self, args): #Done2
        query = '''
            select CODE_ZONE,NOM_ZONE,SP.NOM_OPERATEUR AS SUP,RV.NOM_OPERATEUR AS RVENTE from T_ZONE,T_OPERATEUR SP,T_OPERATEUR RV
            where SP.CODE_OPERATEUR = T_ZONE.CODE_SUPERVISEUR
            and RV.CODE_OPERATEUR = T_ZONE.RESP_VENTE
        '''

        return query


    def Req_conv_generation_in_6(self, args): #Done2
        query = '''
            select CODE_CAT_CLIENT,NOM_CATEGORIE from T_CAT_CLIENTS
        '''

        return query


    def Req_conv_generation_in_7(self, args): #Done2
        query = '''
            select T_CLIENTS.CODE_CLIENT,NOM_CLIENT,CAT_CLIENT,CLASSE,TELEPHONE,CODE_SECTEUR,TYPE_PRESENTOIRE,(select COUNT(*) from T_ITINERAIRES,T_TOURNEES where CODE_CLIENT = T_CLIENTS.CODE_CLIENT and T_ITINERAIRES.CODE_TOURNEE=T_TOURNEES.CODE_TOURNEE and T_TOURNEES.ACTIF=1) AS FRQ,ADRESSE,SOUS_SECTEUR
            from T_CLIENTS,T_SOUS_SECTEUR
            where T_CLIENTS.ACTIF=1 and client_en_compte=0
            and T_CLIENTS.SOUS_SECTEUR = T_SOUS_SECTEUR.CODE_SOUS_SECTEUR
        '''

        return query


    def Req_conv_generation_in_8(self, args): #Done2
        query = '''
            select T_SOUS_SECTEUR.CODE_SECTEUR,T_FACTURE.CODE_OPERATEUR,NOM_OPERATEUR,COUNT(NUM_FACTURE) as NB_FACTURE from T_FACTURE,T_CLIENTS,T_SOUS_SECTEUR,T_OPERATEUR where DATE_HEURE>='20150401' and VALID=1 and T_CLIENTS.CODE_CLIENT=T_FACTURE.CODE_CLIENT and T_CLIENTS.SOUS_SECTEUR=T_SOUS_SECTEUR.CODE_SOUS_SECTEUR
            and T_FACTURE.CODE_OPERATEUR = T_OPERATEUR.CODE_OPERATEUR
            group by CODE_SECTEUR,T_FACTURE.CODE_OPERATEUR,NOM_OPERATEUR order by NB_FACTURE  desc
        '''

        return query


    def Req_conv_generation_in_9(self, args): #Done2
        query = '''
            select CODE_CLIENT,SUM(QTE_VENTE*PRIX)/('{param_nbj}') AS CA_MOY,COUNT( distinct T_FACTURE.NUM_FACTURE) as NB_FACTS,COUNT( distinct T_ARTICLES.CODE_PRODUIT) as NB_PRODUITS
            from T_FACTURE,T_DT_FACTURE,T_ARTICLES
            where T_FACTURE.NUM_FACTURE=T_DT_FACTURE.NUM_FACTURE
            and T_ARTICLES.CODE_ARTICLE = T_DT_FACTURE.CODE_ARTICLE
            and DATE_HEURE between '{param_dt1}' and '{param_dt2}'
            group by CODE_CLIENT
        '''

        try:
            kwargs = {
                'param_nbj': args[0],
                'param_dt1': args[1],
                'param_dt2': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['param_dt1'] = self.validateDate(kwargs['param_dt1'], 0)
        kwargs['param_dt2'] = self.validateDate(kwargs['param_dt2'], 1)

        if kwargs['param_nbj'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)
    

    def Req_conv_affiche_ls_inventaire_nom_magasin(self, args): #Done2
        query = '''
            SELECT
                T_MAGASINS.NOM_MAGASIN AS NOM_MAGASIN
            FROM
                T_MAGASINS
            WHERE
                T_MAGASINS.CODE_MAGASIN = {code_magasin}
        '''

        try:
            kwargs = {
                'code_magasin': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['code_magasin'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)
    

    def Req_conv_affiche_ls_inventaire_nom_operateur(self, args): #Done2
        query = '''
            SELECT
                T_OPERATEUR.NOM_OPERATEUR AS NOM_OPERATEUR
            FROM
                T_OPERATEUR
            WHERE
                T_OPERATEUR.CODE_OPERATEUR = {code_operateur}
        '''

        try:
            kwargs = {
                'code_operateur': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['code_operateur'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_fen_inventaire_btn_supprimer_compte_ecart(self, args): #Done2
        query = '''
            SELECT
                T_MOUVEMENTS_CAISSERIE.COMPTE_ECART AS COMPTE_ECART
            FROM
                T_MOUVEMENTS_CAISSERIE
            WHERE
                T_MOUVEMENTS_CAISSERIE.ID_MOUVEMENT = {id_mouvement}
        '''

        try:
            kwargs = {
                'id_mouvement': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['id_mouvement'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_fen_inventaire_btn_supprimer_compte_ecart2(self, args): #Done2
        query = '''
            SELECT
                T_MOUVEMENTS.COMPTE_ECART AS COMPTE_ECART
            FROM
                T_MOUVEMENTS
            WHERE
                T_MOUVEMENTS.ID_MOUVEMENT = {id_mouvement}
        '''

        try:
            kwargs = {
                'id_mouvement': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['id_mouvement'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_fen_inventaire_btn_supprimer_code_operation(self, args): #Done2
        query = '''
            SELECT
                T_OPERATIONS.CODE_OPERATION AS CODE_OPERATION
            FROM
                T_OPERATIONS
            WHERE
                T_OPERATIONS.CODE_OPERATION = {code_operation}
        '''

        try:
            kwargs = {
                'code_operation': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['code_operation'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_nouv_journee_date_journee(self, args): #Done2
        query = '''
            SELECT DISTINCT
                T_JOURNEE.DATE_JOURNEE AS DATE_JOURNEE
            FROM
                T_JOURNEE
            WHERE
                T_JOURNEE.CODE_AGCE = {code_agce}
        '''

        try:
            kwargs = {
                'code_agce': args[0]
            }
        except IndexError as e:
            return e
        
        if kwargs['code_agce'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_nouv_journee_articles_magasins(self, args): #Done2
        query = '''
            SELECT
                T_ARTICLES_MAGASINS.QTE_STOCK AS QTE_STOCK,
                T_ARTICLES_MAGASINS.CATEGORIE AS CATEGORIE,
                T_ARTICLES_MAGASINS.CODE_ARTICLE AS CODE_ARTICLE,
                T_ARTICLES_MAGASINS.MAGASIN AS MAGASIN
            FROM
                T_ARTICLES_MAGASINS
        '''
        
        return query


    def Req_conv_journee_btn_nouv_journee_stock_init(self, args): #Done2
        query = '''
            INSERT INTO 
                T_STOCK_INIT (DATE_PS, CATEGORIE, CODE_ARTICLE, CODE_MAGASIN, QTE_INIT)
            VALUES
                ('{date_ps}', '{categorie}', {code_article}, {code_magasin}, {qte_init})
        '''

        try:
            kwargs = {
                'date_ps': args[0],
                'categorie': args[1],
                'code_article': args[2],
                'code_magasin': args[3],
                'qte_init': args[4]
            }
        except IndexError as e:
            return e
        
        kwargs['date_ps'] = self.validateDate(kwargs['date_ps'])
        
        if kwargs['date_ps'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_nouv_journee_magasin_cond(self, args): #Done2
        query = '''
            SELECT
                T_MAGASIN_COND.CODE_MAGASIN AS CODE_MAGASIN,
                T_MAGASIN_COND.CODE_CP AS CODE_CP,
                T_MAGASIN_COND.QTE_STOCK AS QTE_STOCK
            FROM
                T_MAGASIN_COND
        '''
        
        return query


    def Req_conv_journee_btn_nouv_journee_stock_initi_cond(self, args): #Done2
        query = '''
            INSERT INTO 
                T_STOCK_INITI_COND (DATE_JOURNEE, CODE_MAGASIN, CODE_CP, STOCK_INIT)
            VALUES
                ('{date_journee}', {code_magasin}, {code_cp}, {stock_init})
        '''

        try:
            kwargs = {
                'date_journee': args[0],
                'code_magasin': args[1],
                'code_cp': args[2],
                'stock_init': args[3]
            }
        except IndexError as e:
            return e
        
        kwargs['date_journee'] = self.validateDate(kwargs['date_journee'])
        
        if kwargs['date_journee'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_nouv_journee_preparation_chargements(self, args): #Done2
        query = '''
            DELETE FROM
                T_PREPARATION_CHARGEMENTS
        '''
        
        return query


    def Req_conv_journee_btn_nouv_journee_journee(self, args): #Done2
        query = '''
            INSERT INTO 
                T_JOURNEE
                (CODE_AGCE, DATE_JOURNEE, SOLDE_EMB, STOCK, SOLDE_CAISSERIE, JOURNEE_TEMP, CLOTURE, AS_C_STD, AS_C_PR,
                AS_C_AG, AS_P_AG, AS_P_UHT, AS_P_EURO, AS_CS_BLC, AS_CS1, AS_CS2, TEMP_MAX, TEMP_MIN, PLUV, COMMENTAIRE)
            VALUES
                ({code_agce}, '{date_journee}', {solde_emb}, {stock}, {solde_caisserie}, {journee_temp}, {cloture}, {as_c_std}, {as_c_pr},
                {as_c_ag}, {as_p_ag}, {as_p_uht}, {as_p_euro}, {as_cs_blc}, {as_cs1}, {as_cs2}, {temp_max}, {temp_min}, {pluv}, '{commentaire}')
        '''

        try:
            kwargs = {
                'code_agce': args[0],
                'date_journee': args[1],
                'solde_emb': args[2],
                'stock': args[3],
                'solde_caisserie': args[4],
                'journee_temp': args[5],
                'cloture': args[6],
                'as_c_std': args[7],
                'as_c_pr': args[8],
                'as_c_ag': args[9],
                'as_p_ag': args[10],
                'as_p_uht': args[11],
                'as_p_euro': args[12],
                'as_cs_blc': args[13],
                'as_cs1': args[14],
                'as_cs2': args[15],
                'temp_max': args[16],
                'temp_min': args[17],
                'pluv': args[18],
                'commentaire': args[19]
            }
        except IndexError as e:
            return e
        
        kwargs['date_journee'] = self.validateDate(kwargs['date_journee'])
        
        if kwargs['date_journee'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_cloturer_livraision_planning(self, args): #Done2
        query = '''
            SELECT
                T_LIVRAISON_PLANNING.CODE_CLIENT AS CODE_CLIENT,
                T_LIVRAISON_PLANNING.LUNDI AS LUNDI,
                T_LIVRAISON_PLANNING.MARDI AS MARDI,
                T_LIVRAISON_PLANNING.MERCREDI AS MERCREDI,
                T_LIVRAISON_PLANNING.JEUDI AS JEUDI,
                T_LIVRAISON_PLANNING.VENDREDI AS VENDREDI,
                T_LIVRAISON_PLANNING.SAMEDI AS SAMEDI,
                T_LIVRAISON_PLANNING.DIMANCHE AS DIMANCHE
            FROM
                T_LIVRAISON_PLANNING
        '''
        
        return query


    def Req_conv_journee_btn_cloturer_synthese_livraision(self, args): #Done2
        query = '''
            INSERT INTO 
                T_SYNTHESE_LIVRAISON 
                (DATE_JOURNEE, CODE_AGCE, CODE_CLIENT, COMMANDE, LIVRE, PROGRAMME, MOTIF_NON_COMMANDE)
            VALUES
                ('{date_journee}', {code_agce}, {code_client}, {commande}, {livre}, {programme}, {motif_non_commande})
        '''

        try:
            kwargs = {
                'date_journee': args[0],
                'code_agce': args[1],
                'code_client': args[2],
                'commande': args[3],
                'livre': args[4],
                'programme': args[5],
                'motif_non_commande': args[6]
            }
        except IndexError as e:
            return e
        
        kwargs['date_journee'] = self.validateDate(kwargs['date_journee'])
        
        if kwargs['date_journee'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_cloturer_creer_journal(self, args): #Done2
        query = '''
            INSERT INTO 
                T_HISTORIQUE_OPERATIONS 
                (CODE_OPERATEUR, COMMENTAIRE, DATE_HEURE, POSTE, SESSION)
            VALUES
                ({code_operateur}, {commentaire}, '{date_heure}', {poste}, {session})
        '''

        try:
            kwargs = {
                'code_operateur': args[0],
                'commentaire': args[1],
                'date_heure': args[2],
                'poste': args[3],
                'session': args[4]
            }
        except IndexError as e:
            return e
        
        kwargs['date_heure'] = self.validateDate(kwargs['date_heure'])
        
        if kwargs['date_heure'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_cloturer_creer_synchro(self, args): #Done2
        query = '''
            INSERT INTO 
                T_SYNCHRO 
                (AGCE, DH_CREATION, ETAT, ID_OPERATION, SOUS_OPERATION, OPERATION)
            VALUES
                ({agce}, '{dh_creation}', {etat}, {id_operation}, {sous_operation}, {operation})
        '''

        try:
            kwargs = {
                'agce': args[0],
                'dh_creation': args[1],
                'etat': args[2],
                'id_operation': args[3],
                'sous_operation': args[4],
                'operation': args[5]
            }
        except IndexError as e:
            return e
        
        kwargs['dh_creation'] = self.validateDate(kwargs['dh_creation'])
        
        if kwargs['dh_creation'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_cloturer_journee_temp(self, args): #Done2
        query = '''
            UPDATE 
                T_JOURNEE
            SET
                T_JOURNEE.JOURNEE_TEMP = {journee_temp}
            WHERE 
                T_JOURNEE.DATE_JOURNEE = '{Param_date_journee}'
                AND T_JOURNEE.CODE_AGCE = {code_agce}
        '''

        try:
            kwargs = {
                'journee_temp': args[0],
                'Param_date_journee': args[1],
                'code_agce': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_date_journee'] = self.validateDate(kwargs['Param_date_journee'])
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_cloturer_solde_initial_caisse(self, args): #Done2
        query = '''
            INSERT INTO 
                T_SOLDE_INITIAL_CAISSE 
                (CODE_CAISSE, DATE_JOURNEE, SOLDE_INITIAL)
            VALUES
                ({code_caisse}, '{date_journee}', {solde_initial})
        '''

        try:
            kwargs = {
                'code_caisse': args[0],
                'date_journee': args[1],
                'solde_initial': args[2]
            }
        except IndexError as e:
            return e
        
        kwargs['date_journee'] = self.validateDate(kwargs['date_journee'])
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_cloturer_controle_cloture(self, args): #Done2
        query = '''
            select A.code_op,COUNT(A.code_op) as nb from (
            select DATE_CHARGEMENT,CODE_VENDEUR as CODE_OP from T_CHARGEMENT where CODE_VENDEUR<>0
            union all
            select DATE_CHARGEMENT,CODE_CHAUFFEUR as CODE_OP from T_CHARGEMENT where CODE_CHAUFFEUR<>0
            union all
            select DATE_CHARGEMENT,AIDE_VENDEUR1 as CODE_OP from T_CHARGEMENT where AIDE_VENDEUR1<>0
            union all
            select DATE_CHARGEMENT,AIDE_VENDEUR2 as CODE_OP from T_CHARGEMENT where AIDE_VENDEUR2<>0
            ) A where A.DATE_CHARGEMENT='{Param_dt}' group by a.CODE_OP having COUNT(A.code_op)>1
        '''

        try:
            kwargs = {
                'Param_dt': args[0]
            }
        except IndexError as e:
            return e
        
        kwargs['Param_dt'] = self.validateDate(kwargs['Param_dt'])
        
        if kwargs['Param_dt'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_import_moy_vente_article(self, args): #Done2
        query = '''
            INSERT INTO 
                T_MOY_VENTE_ARTICLE 
                (CODE_PRODUIT, CODE_SECTEUR, DATE_VENTE, QTE_INVENDU, QTE_PERTE, QTE_VENTE)
            VALUES
                ({code_produit}, {code_secteur}, '{date_vente}', {qte_invendu}, {qte_perte}, {qte_vente})
        '''

        try:
            kwargs = {
                'code_produit': args[0],
                'code_secteur': args[1],
                'date_vente': args[2],
                'qte_invendu': args[3],
                'qte_perte': args[4],
                'qte_vente': args[5]
            }
        except IndexError as e:
            return e
        
        kwargs['date_vente'] = self.validateDate(kwargs['date_vente'])
        
        if kwargs['date_vente'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_journee_btn_import_moy_vente_clients(self, args): #Done2
        query = '''
            INSERT INTO 
                T_MOY_VENTE_ARTICLE 
                (CODE_CLIENT, CODE_PRODUIT, DATE_VENTE, QTE_VENTE, QTE_PERTE)
            VALUES
                ({code_client}, {code_produit}, '{date_vente}', {qte_vente}, {qte_perte})
        '''

        try:
            kwargs = {
                'code_client': args[0],
                'code_produit': args[1],
                'date_vente': args[2],
                'qte_vente': args[3],
                'qte_perte': args[4]
            }
        except IndexError as e:
            return e
        
        kwargs['date_vente'] = self.validateDate(kwargs['date_vente'])
        
        if kwargs['date_vente'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_codification_operation_cod_std(self, args): #Done2
        #HLitRecherche(T_AGENCE,CODE_AGCE,var_code_agce)
        query = '''
            SELECT
                T_AGENCE.COD_STD AS COD_STD
            FROM
                T_AGENCE
            WHERE
                T_AGENCE.CODE_AGCE = {code_agce}
        '''

        try:
            kwargs = {
                'code_agce': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['code_agce'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)


    def Req_conv_fen_inventaire_btn_appliquer_operations(self, args): #Done2
        query = '''
            INSERT INTO 
                T_OPERATIONS
                (CODE_OPERATION, TYPE_OPERATION, DATE_OPERATION, DATE_HEURE, CODE_OPERATEUR, COMMENTAIRE, NUM_CONVOYAGE,
                TYPE_PRODUIT, CODE_AGCE1, CODE_MAGASIN1, COMPTE_ECART, REF)
            VALUES
                ({code_operation}, '{type_operation}', '{date_operation}', '{date_heure}', {code_operateur}, '{commentaire}',
                {num_convoyage}, {type_produit}, {code_agce1}, {code_magasin1}, {compte_ecart}, '{ref}')
        '''

        try:
            kwargs = {
                'code_operation': args[0],
                'type_operation': args[1],
                'date_operation': args[2],
                'date_heure': args[3],
                'code_operateur': args[4],
                'commentaire': args[5],
                'num_convoyage': args[6],
                'type_produit': args[7],
                'code_agce1': args[8],
                'code_magasin1': args[9],
                'compte_ecart': args[10],
                'ref': args[11]
            }
        except IndexError as e:
            return e
        
        kwargs['date_operation'] = self.validateDate(kwargs['date_operation'])
        kwargs['date_heure'] = self.validateDate(kwargs['date_heure'])
        
        if kwargs['date_operation'] in (None, 'NULL') or kwargs['date_heure'] in (None, 'NULL'):
            return ValueError
        
        return query.format(**kwargs)


    def Req_conv_fen_inventaire_btn_appliquer_mouvements_caisserie(self, args): #Done2
        query = '''
            INSERT INTO 
                T_MOUVEMENTS_CAISSERIE
                (ID_MOUVEMENT, CODE_MAGASIN, CODE_CP, ORIGINE, QTE_THEORIQUE, QTE_REEL, QTE_ECART,
                PRIX, MONTANT_ECART, CODE_OPERATEUR, COMPTE_ECART, TYPE_MOUVEMENT, QTE_MOUVEMENT)
            VALUES
                ({id_mouvement}, {code_magasin}, {code_cp}, {origine}, {qte_theorique}, {qte_reel}, {qte_ecart}, 
                {prix}, {montant_ecart}, {code_operateur}, {compte_ecart}, '{type_mouvement}', {qte_mouvement})
        '''

        try:
            kwargs = {
                'id_mouvement': args[0],
                'code_magasin': args[1],
                'code_cp': args[2],
                'origine': args[3],
                'qte_theorique': args[4],
                'qte_reel': args[5],
                'qte_ecart': args[6],
                'prix': args[7],
                'montant_ecart': args[8],
                'code_operateur': args[9],
                'compte_ecart': args[10],
                'type_mouvement': args[11],
                'qte_mouvement': args[12]
            }
        except IndexError as e:
            return e

        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError

        return query.format(**kwargs)


    def Req_conv_fen_inventaire_btn_appliquer_mouvements(self, args): #Done2
        query = '''
            INSERT INTO 
                T_MOUVEMENTS
                (ID_MOUVEMENT, CODE_MAGASIN, DATE_MVT, TYPE_MOUVEMENT, ORIGINE, CODE_ARTICLE, QTE_THEORIQUE,
                QTE_REEL, QTE_MOUVEMENT, QTE_ECART, PRIX, MONTANT, MONTANT_ECART, COMPTE_ECART, DATE_HEURE_MOUVEMENT, CODE_OPERATEUR)
            VALUES
                ({id_mouvement}, {code_magasin}, '{date_mvt}', '{type_mouvement}', {origine}, {code_article}, {qte_theorique}, 
                {qte_reel}, {qte_mouvement}, {qte_ecart}, {prix}, {montant}, {montant_ecart}, {compte_ecart}, GETDATE(), {code_operateur})
        '''

        try:
            kwargs = {
                'id_mouvement': args[0],
                'code_magasin': args[1],
                'date_mvt': args[2],
                'type_mouvement': args[3],
                'origine': args[4],
                'code_article': args[5],
                'qte_theorique': args[6],
                'qte_reel': args[7],
                'qte_mouvement': args[8],
                'qte_ecart': args[9],
                'prix': args[10],
                'montant': args[11],
                'montant_ecart': args[12],
                'compte_ecart': args[13],
                'code_operateur': args[14]
            }
        except IndexError as e:
            return e

        kwargs['date_mvt'] = self.validateDate(kwargs['date_mvt'])

        if kwargs['date_mvt'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)


    def Req_conv_maj_position_articles_magasins(self, args): #Done2
        query = '''
            INSERT INTO 
                T_ARTICLES_MAGASINS 
                (CODE_ARTICLE, CATEGORIE, MAGASIN, QTE_STOCK)
            VALUES
                ({code_article}, {categorie}, {magasin}, {qte_stock})
        '''

        try:
            kwargs = {
                'code_article': args[0],
                'categorie': args[1],
                'magasin': args[2],
                'qte_stock': args[3]
            }
        except IndexError as e:
            return e
        
        return query.format(**kwargs)


    def Req_conv_maj_position_articles_magasins_qte_stock(self, args): #Done2
        query = '''
            UPDATE 
                T_ARTICLES_MAGASINS
            SET
                T_ARTICLES_MAGASINS.QTE_STOCK = {qte_stock}
            WHERE 
                T_ARTICLES_MAGASINS.PK_T_ARTICLES_MAGASINS  = {pk_t_articles_magasins}
        '''

        try:
            kwargs = {
                'qte_stock': args[0],
                'pk_t_articles_magasins': args[1]
            }
        except IndexError as e:
            return e
        
        for key in kwargs:
            if kwargs[key] in (None, 'NULL'):
                return ValueError
        
        return query.format(**kwargs)


    def Req_conv_codification_mvt_cond_cod_long(self, args): #Done2
        query = '''
            SELECT
                T_AGENCE.COD_LONG AS COD_LONG
            FROM
                T_AGENCE
            WHERE
                T_AGENCE.CODE_AGCE = {code_agce}
        '''

        try:
            kwargs = {
                'code_agce': args[0]
            }
        except IndexError as e:
            return e

        if kwargs['code_agce'] in (None, 'NULL'):
            return ValueError

        return query.format(**kwargs)
