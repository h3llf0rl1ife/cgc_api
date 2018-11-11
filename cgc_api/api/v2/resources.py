import pyodbc
from flask import request
from flask_restful import Resource

from cgc_api.queries import Queries
from cgc_api.config import CURRENT_CONFIG, HTTP_STATUS, SECRET, STAT_TABLES
from cgc_api import app  # , db, models as v1m
# from cgc_api.api.v2 import models as m
from cgc_api.crypto import Crypto
from cgc_api.api.v2.query import Query
from cgc_api.resources import removeSQLInjection
from cgc_api.auth import getDate


class RestfulSchemaV2(Resource):
    def get(self):
        schema = {
            'Version': 2,
            'URI': '/api/v2',
            'Resources': {
                '/queries': {
                    'Parameters': [{
                        'query': '<query name>',
                        'kwargs': ['arg1', 'arg2']
                    }],
                    'Token': '<Token>'
                },
                '/query/<table>': {
                    'Method': '<HTTP Method>',
                    'Parameters': [{
                        'Select': {
                            'Columns': '<column1>, <column2>',
                            'Where': {
                                '<operator>': [
                                    '<column>:<value>; <column>:<value>'
                                ]
                            },
                        },
                        'Insert': {
                            'Values': [
                                '<column>:<value>; <column>:<value>'
                            ]
                        },
                        'Update': {
                            'Values': [
                                '<column>:<value>; <column>:<value>'
                            ],
                            'Where': {
                                '<operator>': [
                                    '<column>:<value>; <column>:<value>'
                                ]
                            }
                        },
                        'Delete': {
                            'Where': {
                                '<operator>': [
                                    ['<column>', '<value>']
                                ]
                            }
                        }
                    }],
                    'Token': '<Token>'
                }
            },
            'HTTP Methods': ['GET', 'POST', 'PUT', 'DELETE'],
            'Operators': {
                'Equal': '=',
                'NotEqual': ['!=', '<>'],
                'GreaterThan': '>',
                'GreaterEqual': '>=',
                'LessThan': '<',
                'LessEqual': '<=',
                'Like': 'LIKE',
                'ILike': 'ILIKE',
                'NotLike': 'NOT LIKE',
                'NotILike': 'NOT ILIKE'
            }
        }
        return schema


class QueriesAPI_V2(Resource):
    queries = Queries(*CURRENT_CONFIG)

    def get(self):
        pass

    def post(self):
        jwt = request.data.decode('utf-8')
        params, agency = None, 0
        responses = list()

        if jwt:
            crypto = Crypto(SECRET + getDate())
            header, payload = crypto.readJWT(jwt)

            if payload:
                # token = payload.get('Token')

                """if token:
                    token = v1m.Token.query.filter_by(TokenHash=token).first()
                    if token:
                        if token.IssuedAt.date().isoformat() != getDate():
                            return {'Status': 498,
                                    'Message': 'Token expired'}, 498

                if not token:
                    return {'Status': 401,
                            'Message': 'Unauthorized access'}, 401"""

                params = payload.get('Parameters')
                agency = payload.get('Agency')

        if params:
            for param in params:
                query = param.get('query')
                kwargs = param.get('kwargs')
                log = '{} {} - Query: {} - Data: {}'.format(
                        request.environ['REMOTE_ADDR'],
                        type(self).__name__, query, kwargs)

                if query:
                    try:
                        query = getattr(self.queries, query)
                    except AttributeError as e:
                        app.logger.warning(log)
                        app.log_exception(e)
                        responses.append([HTTP_STATUS['477'], 477])

                    if kwargs:
                        kwargs = [removeSQLInjection(kwarg)
                                  for kwarg in kwargs]
                        kwargs.append(agency)
                    else:
                        kwargs = [agency]

                    try:
                        responses.append(
                            [self.queries.executeQuery(query(kwargs)), 200])

                    except (IndexError, ValueError) as e:
                        app.logger.warning(log)
                        app.log_exception(e)
                        responses.append([HTTP_STATUS['472'], 472])

                    except (pyodbc.ProgrammingError,
                            pyodbc.OperationalError) as e:
                        app.logger.warning(log)
                        app.log_exception(e)
                        responses.append([HTTP_STATUS['488'], 488])

                    except pyodbc.IntegrityError as e:
                        app.logger.warning(log)
                        app.log_exception(e)
                        responses.append([HTTP_STATUS['487'], 487])

            return responses, 200

        return HTTP_STATUS['471'], 471


class RestfulQuery_V2:
    _Query = Query(*CURRENT_CONFIG)

    def __init__(self, data):
        self.data = data

    def get(self, table):
        data = self.data
        args = list()
        log = '{} - {} - Table: {} - Data: {}'.format(
                request.environ['REMOTE_ADDR'], type(self).__name__,
                table, data)

        if data:
            s_columns = data['Parameters']['Select'].get('Columns')
            args.append(s_columns)

            where_data = data['Parameters']['Select'].get('Where')
            w_values, w_columns = list(), list()
            if where_data:
                for operator in where_data:
                    colvals = where_data[operator].split(';')
                    columns = list()

                    for cv in colvals:
                        cv = cv.split('^')
                        columns.append(cv[0])
                        w_values.append(cv[1])

                    w_columns.append([operator, columns])
                args.append(w_columns)

        query = self._Query.getRequest(table, *args)

        try:
            return self._Query.executeQuery(
                query=query, params=tuple(w_values)), 200

        except pyodbc.OperationalError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['473'], 473

        except pyodbc.ProgrammingError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['488'], 488

    def delete(self, table):
        data = self.data
        args = list()
        log = '{} - {} - Table: {} - Data: {}'.format(
                request.environ['REMOTE_ADDR'], type(self).__name__,
                table, data)

        if data:
            where_data = data['Parameters']['Delete'].get('Where')
            w_values, w_columns = list(), list()

            if where_data:
                for operator in where_data:
                    colvals = where_data[operator].split(';')
                    columns = list()

                    for cv in colvals:
                        cv = cv.split('^')
                        columns.append(cv[0])
                        w_values.append(cv[1])

                    w_columns.append([operator, columns])
                args.append(w_columns)

        query = self._Query.deleteRequest(table, *args)

        try:
            row_count = self._Query.executeQuery(
                query=query, params=tuple(w_values), with_result=False)

            return {'Status': 200,
                    'Message': 'Deleted {} records from {}.'.format(
                        row_count, table)}, 200

        except pyodbc.OperationalError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['471'], 471

        except pyodbc.ProgrammingError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['488'], 488

    def post(self, table):
        data = self.data
        args = list()
        log = '{} - {} - Table: {} - Data: {}'.format(
                request.environ['REMOTE_ADDR'], type(self).__name__,
                table, data)

        if data:
            insert_data = data['Parameters']['Insert'].get('Values')
            i_values, i_columns = list(), list()

            if insert_data:
                colvals = insert_data.split(';')

                for cv in colvals:
                    cv = cv.split('^')
                    i_columns.append(cv[0])
                    i_values.append(cv[1])

            args.append(i_columns)

        query = self._Query.postRequest(table, *args)

        try:
            row_count = self._Query.executeQuery(
                query=query, params=tuple(i_values), with_result=False)

            return {'Status': 200,
                    'Message': 'Inserted {} records into {}.'.format(
                        row_count, table)}, 200

        except pyodbc.OperationalError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['471'], 471

        except pyodbc.ProgrammingError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['488'], 488

        except pyodbc.IntegrityError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['487'], 487

    def put(self, table):
        data = self.data
        args = list()
        log = '{} - {} - Table: {} - Data: {}'.format(
                request.environ['REMOTE_ADDR'], type(self).__name__,
                table, data)

        if data:
            update_data = data['Parameters']['Update'].get('Values')
            u_values, u_columns = list(), list()

            if update_data:
                colvals = update_data.split(';')

                for cv in colvals:
                    cv = cv.split('^')
                    u_columns.append(cv[0])
                    u_values.append(cv[1])

            args.append(u_columns)

            where_data = data['Parameters']['Update'].get('Where')
            w_values, w_columns = list(), list()

            if where_data:
                for operator in where_data:
                    colvals = where_data[operator].split(';')
                    columns = list()

                    for cv in colvals:
                        cv = cv.split('^')
                        columns.append(cv[0])
                        w_values.append(cv[1])

                    w_columns.append([operator, columns])
                args.append(w_columns)

        values = tuple(u_values + w_values)
        query = self._Query.putRequest(table, *args)

        try:
            row_count = self._Query.executeQuery(
                query=query, params=values, with_result=False)
            return {'Status': 200,
                    'Message': 'Updated {} record in {}.'.format(
                        row_count, table)}, 200

        except pyodbc.OperationalError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['471'], 471

        except pyodbc.ProgrammingError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['488'], 488

        except pyodbc.IntegrityError as e:
            app.logger.warning(log)
            app.log_exception(e)
            return HTTP_STATUS['487'], 487


class DatabaseAPI_V2(Resource):
    def respond(self, table):
        jwt = request.data.decode('utf-8')
        params = None
        responses = list()

        if jwt:
            crypto = Crypto(SECRET + getDate())
            header, payload = crypto.readJWT(jwt)

            if payload:
                # token = payload.get('Token')

                """if token:
                    token = v1m.Token.query.filter_by(TokenHash=token).first()
                    if token:
                        if token.IssuedAt.date().isoformat() != getDate():
                            return {'Status': 498,
                                    'Message': 'Token expired'}, 498

                if not token:
                    return {'Status': 401,
                            'Message': 'Unauthorized access'}, 401"""

                params = payload.get('Parameters')

        if params:
            for param in params:
                param = {'Parameters': param}
                log = '{} {} Data: {}'.format(
                        request.environ['REMOTE_ADDR'],
                        type(self).__name__, param)

                restfulQuery = RestfulQuery_V2(param)

                if table in STAT_TABLES:
                    args = CURRENT_CONFIG[:3] + ('STATISTIQUES',)
                    restfulQuery._Query = Query(*args)
                elif table == 'P_TIERS':
                    args = ('10.7.0.20',) + CURRENT_CONFIG[1:3] + ('GCOPAG',)
                    restfulQuery._Query = Query(*args)
                else:
                    restfulQuery._Query = Query(*CURRENT_CONFIG)

                methods = {
                    'GET': restfulQuery.get,
                    'POST': restfulQuery.post,
                    'PUT': restfulQuery.put,
                    'DELETE': restfulQuery.delete
                }
                method = payload.get('Method')

                try:
                    response, status_code = methods[method](table)
                except KeyError as e:
                    responses.append([HTTP_STATUS['405'], 405])
                    app.logger.warning(log)
                    app.log_exception(e)
                    continue

                responses.append([response, status_code])

            return responses, 200
        return HTTP_STATUS['471'], 471

    def get(self, table):
        return self.respond(table)

    def post(self, table):
        return self.respond(table)


'''class Journee(Resource):
    queries = Queries(*CURRENT_CONFIG)

    def get(self, agence):
        journees = m.Journee.query.filter_by(
            code_agce=agence).order_by(
                m.Journee.date_journee.desc()).limit(10).all()

        return [{'date_journee': j.date_journee.isoformat(),
                 'stock': j.stock,
                 'cloture': j.cloture,
                 'solde_emb': j.solde_emb,
                 'solde_caisse': j.solde_caisse,
                 'solde_caisserie': j.solde_caisserie}
                for j in journees], 200

    def post(self, action):
        # Pre-process
        jwt = request.data.decode('utf-8')
        payload = {}

        if jwt:
            crypto = Crypto(SECRET + getDate())
            header, payload = crypto.readJWT(jwt)

        if not payload:
            return HTTP_STATUS[400], 400

        machine = payload.get('Machine')
        session = payload.get('Session')
        operator = payload.get('Operator')
        agency = payload.get('Agency')
        store = payload.get('Store')
        caisse_p = payload.get('Caisse_p')
        caisse_d = payload.get('Caisse_d')
        j_date_in = payload.get('Date')

        if action == 'Create':
            j_count = len(m.Journee.query.filter_by(
                code_agce=agency, cloture=False).order_by(
                    m.Journee.date_journee.desc()).all())

            if j_count > 1:
                return {'Message': 'Impossible de dépasser \
                                    deux journées non clôturées.'}, 400

            j_date = m.Journee.query.filter(
                m.Journee.date_journee >= j_date_in).first()

            if j_date:
                return {'Message': 'Journée déjà existante.'}, 400

            j_max = m.Journee.query.filter_by(
                code_agce=agency).order_by(
                    m.Journee.date_journee.desc()).first()

            if j_max:
                j_max = j_max.date_journee.date()
            else:
                j_max = j_date_in

            j_max_plus = j_max + datetime.timedelta(days=1)

            if j_date_in.day() == 1:
                j_import_1 = j_date_in.replace(day=1, year=j_date_in.year - 1)
                j_import = j_date_in.replace(day=1)
                j_date = m.Journee.query.filter_by(
                    code_agce=agency, date_journee=j_import).first()

                j_check = True
                if j_date:
                    if (j_date.cloture or j_date.solde_emb
                            or j_date.stock or j_date.solde_caisserie):
                        j_check = False

                if j_check:
                    j_import_31 = j_import.replace(
                        day=calendar.monthrange(
                            j_import.year, j_import.month)[1])

                    zones = m.Agence.query.get(agency).zones

                    for zone in zones:
                        blocs = zone.blocs
                        for bloc in blocs:
                            secteurs = bloc.secteurs
                            for secteur in secteurs:
                                mva = m.MoyVenteArticle.query.filter_by(
                                    date_vente=j_import_1,
                                    code_secteur=secteur.code_secteur).all()
                                for i in mva:
                                    db.session.delete(i)
                                    # db.session.commit()

                                s_secteurs = secteur.sousSecteurs
                                for s_secteur in s_secteurs:
                                    clients = s_secteur.clients
                                    for client in clients:
                                        mvc = m.MoyVenteClients.query.filter_by(
                                            date_vente=j_import_1,
                                            code_client=client.code_client).all()
                                        for i in mvc:
                                            db.session.delete(i)
                                            # db.session.commit()

                    j_diff = j_import_31 - j_import
                    j_diff = j_diff.days

                    query = self.queries.Req_total_chargement(
                        (j_import, j_import_31, agency))
                    j_total_charg = self.queries.executeQuery(query)

                    for line in j_total_charg:
                        sum_TIP = line.get('la_somme_TOTAL_INVENDU_POINTE')
                        sum_TRC = line.get('la_somme_TOTAL_RENDUS_COM')
                        sum_TV = line.get('la_somme_TOTAL_VENDU')

                        if 0 not in (sum_TIP, sum_TRC, sum_TV):
                            mva_new = m.MoyVenteArticle(
                                code_produit=line.get('CODE_ARTICLE'),
                                code_secteur=line.get('CODE_SECTEUR'),
                                date_vente=j_import,
                                qte_invendu=sum_TIP / j_diff,
                                qte_perte=sum_TRC / j_diff,
                                qte_vente=sum_TV / j_diff)
                            db.session.add(mva_new)
                            # db.session.commit()

            if j_max_plus != j_date_in:
                # return error
                pass

            j_temp = True if j_count else False
            j_new = m.Journee(code_agce=agency, date_journee=j_date_in,
                              solde_emb=False, solde_caisserie=False,
                              stock=False, temp=j_temp, cloture=False)

            j_before = m.Journee.query.filter_by(
                date_journee=j_date_in - 1, cloture=True,
                code_agce=agency).first()

            if j_before:
                j_new.as_c_std = j_before.ns_c_std
                j_new.as_c_pr = j_before.ns_c_pr
                j_new.as_c_ag = j_before.ns_c_ag
                j_new.as_p_ag = j_before.ns_p_ag
                j_new.as_p_uht = j_before.ns_p_uht
                j_new.as_p_euro = j_before.ns_pal_euro
                j_new.as_cs_blc = j_before.ns_cs_blc
                j_new.as_cs1 = j_before.nv_cs1
                j_new.as_cs2 = j_before.nv_cs2

            # db.session.add(j_new)

            stock_init = m.StockInit.query.filter_by(
                date_ps=j_date_in, code_agce=agency).all()
            # db.session.delete(stock_init)

            stock_init_cond = m.StockInitCond.filter_by(
                date_ps=j_date_in, code_agce=agency).all()
            # db.session.delete(stock_init_cond)

            articles_magasins = m.ArticlesMagasins.query.filter(
                m.ArticlesMagasins.qte_stock != 0,
                m.ArticlesMagasins.magasin == store).all()

            for article in articles_magasins:
                db.session.add(m.StockInit(
                    date_ps=j_date_in,
                    categorie=article.categorie,
                    code_article=article.code_article,
                    code_magasin=article.magasin,
                    qte_init=article.qte_stock))

            magasin_cond = m.MagasinCond.query.filter(
                m.MagasinCond.code_magasin == store,
                m.MagasinCond.qte_stock != 0).all()

            for mc in magasin_cond:
                db.session.add(m.StockInitCond(
                    date_journee=j_date_in,
                    code_magasin=store,
                    code_cp=mc.code_cp,
                    stock_init=mc.qte_stock))

            prep_charge = m.PreparationChargements.filter_by(
                code_agce=agency).all()
            db.session.delete(prep_charge)
            # db.session.commit()

        elif action == 'Close':
            # controle cloture
            stock = 'Selected entry in table'
            solde_emb = 'Selected entry in table'
            solde_caisserie = 'Selected entry in table'

            j_date_check = m.Journee.query.filter(
                m.Journee.date_journee < j_date_in,
                m.Journee.cloture is False).all()

            if j_date_check:
                # return error
                pass

            repartition = m.Repartition.query.filter_by(
                date_repartition=j_date_in,
                code_agence=agency).filter(
                    m.Repartition.validation != 0).all()

            if repartition:
                # return error
                pass

            query = self.queries.Req_chargement_non_valide((j_date_in))
            j_charge_check = self.queries.executeQuery(query)

            if j_charge_check:
                # return error
                pass

            if not stock:
                # return error
                pass

            if not solde_emb:
                # return error
                pass

            if not solde_caisserie:
                # return error
                pass

            j_date_check = m.Journee.query.filter_by(
                date_journee=j_date_in).first()

            if not j_date_check:
                # return error
                pass
            elif j_date_check.cloture:
                # return error
                pass

            j_date_plus = j_date_in + 1

            prix = m.Prix.query.filter_by(
                code_agce=agency, date_debut=j_date_plus).all()

            for p in prix:
                row = m.PrixAgence.query.filter_by(
                    code_agce=agency,
                    code_article=p.code_article).update(
                        dict(prix_vente=p.prix))
                db.session.commit()

            j_state = m.Journee.query.filter_by(
                date_journee=j_date_plus).update(dict(temp=0))
            db.session.commit()

            # cloture solde caisse
            j_ops_caisse = m.OperationsCaisse.query.filter_by(
                date_validation=j_date_in, code_agce=agency).all()

            solde_d = 0
            for op in j_ops_caisse:
                solde_d += op.montant

            solde_p = -solde_d

            query = self.queries.Req_total_decompte_espece(
                (j_date_in, agency))
            j_decompte = self.queries.executeQuery(query)

            for d in j_decompte:
                solde_p += d.get('la_somme_MONTANT')

            query = self.queries.Req_total_mvt_caisse(
                (None, j_date_in, agency))
            j_mvt_caisse = self.queries.executeQuery(query)

            for mvt in j_mvt_caisse:
                op_type = mvt.get('TYPE_OPERATION')
                code_caisse = mvt.get('CODE_CAISSE')
                montant = mvt.get('la_somme_MONTANT')

                if op_type == 'T' or code_caisse == caisse_p:
                    solde_p -= montant
                elif code_caisse == caisse_d:
                    solde_d -= montant

            j_solde_caisse = m.SoldeInitialCaisse.query.filter_by(
                date_journee=j_date_in).filter(
                    m.SoldeInitialCaisse.code_caisse.in_(
                        (caisse_p, caisse_d))).all()

            for s in j_solde_caisse:
                if s.code_caisse == caisse_p:
                    solde_p += s.solde_initial
                elif s.code_caisse == caisse_d:
                    solde_d += s.solde_initial

            j_solde_caisse = m.SoldeInitialCaisse.query.filter_by(
                date_journee=j_date_plus).filter(
                    m.SoldeInitialCaisse.code_caisse.in_(
                        (caisse_p, caisse_d))).all()

            for s in j_solde_caisse:
                db.session.delete(s)
            db.session.commit()

            db.session.add(m.SoldeInitialCaisse(
                code_caisse=caisse_p,
                date_journee=j_date_plus,
                solde_initial=solde_p))
            db.session.add(m.SoldeInitialCaisse(
                code_caisse=caisse_d,
                date_journee=j_date_plus,
                solde_initial=solde_d))
            db.session.commit()

            j_update = m.Journee.query.filter_by(
                date_journee=j_date_in).update(dict(cloture=True))
            db.session.commit()

            # restart?

            planning = m.LivraisonPlanning.query.filter_by(
                code_agce=agency).all()
            weekday = j_date_plus.weekday()

            for p in planning:
                program = {0: p.lundi, 1: p.mardi, 2: p.mercredi, 3: p.jeudi,
                           4: p.vendredi, 5: p.samedi, 6: p.dimanche}
                program = program.get(weekday)

                # stats

            comment = 'FinTraitement Statistique : {} --> {}'.format(
                j_date_in.strftime('dd/MM/YYYY'))

            stat_check = m.HistoriqueOperations.query.filter_by(
                commentaire=comment).first()

            if not stat_check:
                # return error
                pass

            comment = 'Cloture Journee : {} --> {}'.format(
                j_date_in.strftime('dd/MM/YYYY'))

            journal = m.HistoriqueOperations(
                cat='JRN', commentaire=comment, poste=machine, session=session,
                date_heure=datetime.datetime.now(), code_operateur=operator)
            db.session.add(journal)
            db.session.commit()'''

# TODO: Check operator
