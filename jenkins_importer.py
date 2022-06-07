import os
import re
github_actions = os.listdir('.github/workflows/')
jenkins_files = os.listdir('deployment/jenkins/production/commands/')

g_map = {}
for l in github_actions:
    g_map[l.replace('prod.kube.', '').replace('.yaml', '')] = l

j_map = {}
for l in jenkins_files:
    j_map[l.replace('-values.yaml', '')] = l

not_present = [x for x in g_map.keys() if x not in j_map.keys()]
not_present.sort()
print(not_present)

for uid in not_present:
    text = ""
    with open(f".github/workflows/prod.kube.{uid}.yaml", 'r') as f:
        text = f.read()

    new_text = ""
    with open("deployment/jenkins/production/commands/adminroles-values.yaml", 'r') as f:
        new_text = f.read()
    
    new_text = new_text.replace('adminroles', uid)
    
    cron = "30 16 * * *"
    if 'redshift' in uid:
        cron = "30 18 * * *"
    if 'pgsql' in uid:
        cron = "30 20 * * *"

    cron_str = f'{uid}: "{cron}"'
    new_text = new_text.replace('adminroles: "0 16 * * *"', cron_str)

    if 'deploymentEnabled: false' in text:
        new_text = new_text.replace('deploymentEnabled: true', 'deploymentEnabled: false').replace('jobsEnabled: false', 'jobsEnabled: true')
    else:
        new_text = new_text.replace('deploymentEnabled: false', 'deploymentEnabled: true').replace('jobsEnabled: true', 'jobsEnabled: false')

    if 'memory: "5000Mi"\n                cpu: "1"' in text:
        new_text = new_text.replace('requests:\n    memory: "1500Mi"\n    cpu: "750m"\n  limits:\n    memory: "2000Mi"\n    cpu: "1"', 'requests:\n    memory: "5000Mi"\n    cpu: "1"\n  limits:\n    memory: "7500Mi"\n    cpu: "2"')
    
    with open(f"deployment/jenkins/production/commands/{uid}-values.yaml", 'w') as f:
        f.write(new_text)
    
    text = ''
    new_text = ''
    with open('deployment/jenkins/production/jenkinsfiles/serviceDeployment', 'r') as file:
        text = file.read()
        pat = r"value:'[a-zA-Z0-9,-]*'"
        m = re.search(pat, text)
        vals = m.group(0)
        jobs = vals.split(':')[1][1:-1]
        new_jobs = f"{jobs},{uid}"
        new_vals = f"value:'{new_jobs}'"
        new_text = re.sub(pat, new_vals, text)

    with open('deployment/jenkins/production/jenkinsfiles/serviceDeployment', 'w') as f:
        f.write(new_text)