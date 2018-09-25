import json
from math import ceil
from random import choice, randint
import sys
import pymongo
from pymongo import MongoClient


def create_users(count_users):

    ls_f_names=['Gabriel','Adam','Raphael','Paul','Louis','Louise','Alice', 'Chloe','Emma','Ines']
    ls_l_names=['Martin','Bernard','Dubois','Thomas']

    ls_users=[]
    for i in range(0,count_users):
        f_name=choice(ls_f_names)
        l_name=choice(ls_l_names)
        email=f_name+'.'+l_name+'@mf.fr'

        dict_user={}
        dict_user['f_name'] = f_name
        dict_user['l_name'] = l_name
        dict_user['email']=email
        dict_user['profiles']=[]
        dict_user['widgets']=[]
        ls_users.append(dict_user)
    return ls_users


def create_surveillances(count_surveillances):

    ls_surveillances=[]
    ls_products=['Wind Speed','Humidity','Precipitation', 'Snow Depth','Air Temperature','Dew Point', 'Soil Temperature']
    ls_priority=['P1','P2','P3']
    ls_notification=[True, False]
    ls_operators=['gt','lt']
    i=0
    for s in range(0,count_surveillances):
        dict_surv={}
        dict_surv['title'] = 'Surveillance ' + str(i)
        dict_surv['product'] = choice(ls_products)
        dict_surv['priority'] = choice(ls_priority)
        dict_surv['notification'] = choice(ls_notification)
        dict_threshold={}
        dict_threshold['value']=randint(1,100)
        dict_threshold['operator'] = choice(ls_operators)
        dict_surv['threshold'] = dict_threshold
        dict_position={}
        dict_position['x']=randint(1,50)
        dict_position['y']=randint(1,50)
        dict_surv['position'] = dict_position
        i=i+1
        ls_surveillances.append(dict_surv)
    return ls_surveillances


def get_surveillances_from_mongo():
    pass


def create_profiles():
    ls_surveillances=get_surveillances_from_mongo()

def create_profiles_old(ls_surveillances):
    ls_names_profiles=['South-West coastal summer','Winter Sports Pyrinees', 'Winter Sports Alpes', 'Colza Farmers France', 'Viticulture', 'Decathlon','SNCF','Wild Fires']

    i=0
    ls_profiles=[]
    for n in ls_names_profiles:
        dict_profile={}
        dict_profile['id']=i
        dict_profile['title'] = n
        nr_surveillances_for_profile=randint(5,15)
        ls_surveillances_for_profile=[]
        while len(ls_surveillances_for_profile)<=nr_surveillances_for_profile:
            x_surveillance=choice(ls_surveillances)
            if x_surveillance in ls_surveillances_for_profile:
                pass
            else:
                ls_surveillances_for_profile.append(x_surveillance)
        dict_profile['ls_surveillances'] = ls_surveillances_for_profile

        i=i+1
        ls_profiles.append(dict_profile)
    return ls_profiles


def save_collection(collection_name, ls_objects):
    client=MongoClient('172.0.0.34',27017)
    db=client['mf']
    collection=db[collection_name]

    for o in ls_objects:
        print(o)
        db[collection_name].insert_one(o)
    print('count of documents in collection : ' + collection_name)
    print(db[collection_name].count())

def get_collection(collection_name):
    client=MongoClient('172.0.0.34',27017)
    db=client['mf']
    collection=db[collection_name]

def remove_all_collections():
    client=MongoClient('172.0.0.34',27017)
    db=client['mf']
    db.drop_collection('users')
    db.drop_collection('surveillances')
    db.drop_collection('profiles')

if __name__ == '__main__':

    remove_all_collections()

    ls_users=create_users(10)
    save_collection('users', ls_users)


    ls_surveillances=create_surveillances(30)
    save_collection('surveillances', ls_surveillances)



    # ls_profiles=create_profiles(ls_surveillances)
    #
    # ls_updated_users=[]
    # for u in ls_users:
    #     count_profiles_for_user=randint(1,len(ls_profiles))
    #     ls_profiles_for_user=[]
    #
    #     for x in range(0,count_profiles_for_user):
    #         x_profile = choice(ls_profiles)
    #         if x_profile not in ls_profiles_for_user:
    #             ls_profiles_for_user.append(x_profile)
    #
    #     u['profiles']=ls_profiles_for_user
    #     ls_updated_users.append(u)
    # #print(ls_updated_users[0])
    #
    # ls_updated_users2=[]
    # for u in ls_updated_users:
    #     ls_widgets_for_user=[]
    #     for p in u['profiles']:
    #
    #         # create widget mes surveillances
    #         dict_widget={}
    #         dict_widget['profile_id']=p['id']
    #         dict_widget['title']='Mes Surveillances'
    #         dict_widget['ls_surveillances']=p['ls_surveillances']
    #         ls_widgets_for_user.append(dict_widget)
    #
    #         # create n widgets notifications
    #         count_widgets_notifications=randint(1,3)
    #         for i in range(1,count_widgets_notifications):
    #             count_surveillances_per_widget=randint(2,len(p['ls_surveillances']))
    #             ls_surveillances_per_widget=[]
    #             for x in range(0,count_surveillances_per_widget):
    #                 x_surveillance = choice(p['ls_surveillances'])
    #                 if x_surveillance not in ls_surveillances_per_widget:
    #                     ls_surveillances_per_widget.append(x_surveillance)
    #
    #             dict_widget = {}
    #             dict_widget['profile_id'] = p['id']
    #             dict_widget['title'] = 'Notification Widget ' + str(i)
    #             dict_widget['ls_surveillances'] = ls_surveillances_per_widget
    #             ls_widgets_for_user.append(dict_widget)
    #
    #
    #         # create n widgets visualization
    #         count_widgets_visualization=randint(1,3)
    #         for i in range(1,count_widgets_visualization):
    #             count_surveillances_per_widget=randint(2,len(p['ls_surveillances']))
    #             ls_surveillances_per_widget=[]
    #             for x in range(0, count_surveillances_per_widget):
    #                 x_surveillance=choice(p['ls_surveillances'])
    #                 if x_surveillance not in ls_surveillances_per_widget:
    #                     ls_surveillances_per_widget.append(x_surveillance)
    #
    #             dict_widget = {}
    #             dict_widget['profile_id'] = p['id']
    #             dict_widget['title'] = 'Visualization Widget ' + str(i)
    #             dict_widget['ls_surveillances'] = ls_surveillances_per_widget
    #             ls_widgets_for_user.append(dict_widget)
    #     u['widgets']=ls_widgets_for_user
    #     ls_updated_users2.append(u)
    #
    # #save_collection('users',ls_updated_users2)
    # #save_collection('surveillances', ls_surveillances)
    # #save_collection('profiles', ls_profiles)

    print('work done')




