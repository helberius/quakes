from flask import Flask
from flask_restful import Resource, Api
from flask import request
from flask import jsonify
from app.es_queries import ESQ
from flask import make_response
import time
import datetime

from flask_cors import CORS

app =Flask(__name__)
CORS(app)
api = Api(app)


class QuakesGet(Resource):

    def get(self ,request_type_id):
        print('get')
        print (Resource)
        print('request_type_id: ' , request_type_id)
        dict_response={}
        dict_response['request_type_id']=request_type_id




        if request_type_id=='by_keyword':
            if 'keyword' in request.args:
                keyword = request.args.get('keyword')
                quakes = ESQ.search_quakes_by_keyword_place('usgs', keyword)
                dict_response['quakes'] = quakes['hits']['hits']
                dict_response['message'] = 'Success'
                parameters={}
                parameters['keyword']=keyword
                dict_response['parameters']=parameters
                return make_response(jsonify(dict_response),200)
            else:
                dict_response['message'] = 'Fail, You need to provide the parameter keyword.'
                return make_response(jsonify(dict_response),400)

        elif request_type_id=='summary':
            if 'keyword' in request.args and 'date_init' in request.args and 'date_end' in request.args:
                keyword = request.args.get('keyword')
                date_init = time.mktime(datetime.datetime.strptime(request.args.get('date_init'), "%Y-%m-%d").timetuple())
                date_init = date_init * 1000
                date_end = time.mktime(datetime.datetime.strptime(request.args.get('date_end'), "%Y-%m-%d").timetuple())
                date_end = date_end * 1000
                summary = ESQ.get_summary_per_period_per_place('usgs', keyword,date_init, date_end)
                parameters={}
                parameters['keyword']=keyword
                parameters['date_init'] = request.args.get('date_init')
                parameters['date_end'] = request.args.get('date_end')

                dict_response['parameters']=parameters
                dict_response['quakes'] = summary['hits']['hits']
                dict_response['aggregations'] = summary['aggregations']['group_by_day']['buckets']
                dict_response['message'] = 'Success'
                return make_response(jsonify(dict_response), 200)
            else:
                dict_response['message'] = 'Fail, You need to provide the parameters keyword date_init and date_end.'
                return make_response(jsonify(dict_response), 400)

        elif request_type_id=='get_list_quakes':
            if 'keyword' in request.args and 'key_days' in request.args :
                print('trying to get list of quakes')
                keyword = request.args.get('keyword')
                key_days = request.args.get('key_days')
                ls_quakes = ESQ.get_ls_quakes('usgs', keyword, key_days)
                dict_response['ls_quakes'] = ls_quakes['hits']['hits']
                dict_response['message'] = 'ok'
                return make_response(jsonify(dict_response), 200)

            else:
                dict_response['message'] = 'Fail, You need to provide the parameters keyword and key_days.'
                return make_response(jsonify(dict_response), 400)

        else:
            dict_response['message'] = 'Fail, I can not recognize the request.'
            return make_response(jsonify(dict_response), 400)



class X_Route(Resource):
    def get(self, dummy):
        dict_response={}
        print (dummy)
        import requests
        url='http://localhost:8086/'+dummy
        r=requests.get(url)
        print(r.json())
        return make_response(jsonify(r.json()))

#api.add_resource(QuakesGet,'/<request_type_id>')
api.add_resource(X_Route,'/<path:dummy>')


if __name__ == '__main__':
    app.run(debug=True)

