
import wget
import os.path

files = {'cv': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-cv/cv.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/cv.xml'
         },
         'vacancy': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-vacancy/vacancy.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/vacancy.xml'
         },
         'response': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-response/response.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/response.xml'
         },
         'invitation': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-invitation/invitation.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/invitation.xml'
         },
         'districts': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-districts/districts.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/districts.xml'
         },
         'industries': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-industries/industries.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/industries.xml'
         },
         'organizations': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-organizations/organizations.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/organizations.xml'
         },
         'professions': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-professions/professions.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/professions.xml'
         },
         'regions': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-regions/regions.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/regions.xml'
         },
         'stat-citizens': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-stat-citizens/stat-citizens.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/stat-citizens.xml'
         },
         'stat-company': {
             'url': 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-stat-company/stat-company.xml',
             'dir_path': '/home/jupyter/mnt/s3/project-trudvsem/originals_current/stat-company.xml'
         }
        }

if __name__ == '__main__':
    for k, v in files.items():
        if os.path.isfile(v['dir_path']) == False:
            wget.download(v['url'], out=v['dir_path'])