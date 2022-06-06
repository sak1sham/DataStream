import os
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

    
    with open(f"deployment/jenkins/production/commands/{uid}-values.yaml", 'w') as f:
        f.write(new_text)