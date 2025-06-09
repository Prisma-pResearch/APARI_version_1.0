---
sidebar_position: 2
description: Real-world Federated Learning
---

# Provision

Federated learning needs the cooperation with multiple sites. In practical cases, the sites may be located in different countries, and the communication between them may be restricted by the laws and regulations. 
NVIDIA FLARE utilizes provisioning process and define the roles such as admin, clients to reduce the amount of human coordination involved to set up a federated learning project. A provisioning tool can be configured to create a startup kit for each site.
Here is the example. 

To get more details about NVFlare provision, please refer to [**here**](https://nvflare.readthedocs.io/en/stable/programming_guide/provisioning_system.html#provisioning)


## Provisioning Tool

NVIDIA FLARE provided Open Provision API to fulfill below configurations: 

* network discovery, such as domain names, port numbers or IP addresses

* credentials for authentication, such as certificates of participants and root authority

* authorization policy, such as roles, rights and rules

* tamper-proof mechanism, such as signatures

* convenient commands, such as shell scripts with default command line options to easily start an individual participant

![provision](https://nvflare.readthedocs.io/en/2.2.1/_images/Open_Provision_API.png)

Let's look into each component of the provision process: 

* Provisioner: This is the container that owns all instances such as Project, Workspace, Provision Context, Builders and Participants.  
* Project: A class stores the information of participants.  
* Participant: Each participant is an entity that communicate with other participants inside the FLARE system to fulfill the application.  
 > the name of each participant is unique.
* Builder: Builders are provided as a convenient way to generate commonly used zip files for a typical NVIDIA FLARE system. Developers are encouraged to add / modify or even remove those builders to fit their own requirements. Each builder is a component or process to generate the information used for provision. Developers need to control the order of running builders.  
* Workspace: A file system to store information. Under provision workspace, each Builder is able to access four folders:
  * wip : for working-in-progress
  * kit_dir : a subfolder in wip
  * state : used to persist information between revisions. 
  * resources: for read-only/static information.

* Provision Context : It's a context instance created by Provisioner and can be read / written by all participants and builders. 


## Practical cases
Now let's look into the practical cases:

* Step 1: Create a default `project.yml` file

```bash
## enter the workspace of project
nvflare provision
```
> `HA mode` means there is overseer in the project.
then FLARE will generate a sample yaml file for us.

```yaml
api_version: 3
name: example_project
description: NVIDIA FLARE sample project yaml file

participants:
  # change example.com to the FQDN of the server
  - name: server1
    type: server
    org: nvidia
    fed_learn_port: 8002
    admin_port: 8003
  - name: site-1
    type: client
    org: nvidia
  - name: site-2
    type: client
    org: nvidia
  - name: admin@nvidia.com
    type: admin
    org: nvidia
    role: project_admin

# The same methods in all builders are called in their order defined in builders section
builders:
  - path: nvflare.lighter.impl.workspace.WorkspaceBuilder
    args:
      template_file: master_template.yml
  - path: nvflare.lighter.impl.template.TemplateBuilder
  - path: nvflare.lighter.impl.static_file.StaticFileBuilder
    args:
      # config_folder can be set to inform NVIDIA FLARE where to get configuration
      config_folder: config

      # app_validator is used to verify if uploaded app has proper structures
      # if not set, no app_validator is included in fed_server.json
      # app_validator: PATH_TO_YOUR_OWN_APP_VALIDATOR

      # when docker_image is set to a docker image name, docker.sh will be generated on server/client/admin
      # docker_image:

      # download_job_url is set to http://download.server.com/ as default in fed_server.json.  You can override this
      # to different url.
      # download_job_url: http://download.server.com/

      overseer_agent:
        path: nvflare.ha.dummy_overseer_agent.DummyOverseerAgent
        # if overseer_exists is true, args here are ignored.  Provisioning
        #   tool will fill role, name and other local parameters automatically.
        # if overseer_exists is false, args in this section will be used and the sp_end_point
        # must match the server defined above in the format of SERVER_NAME:FL_PORT:ADMIN_PORT
        # 
        overseer_exists: false
        args:
          sp_end_point: server1:8002:8003

  - path: nvflare.lighter.impl.cert.CertBuilder
  - path: nvflare.lighter.impl.signature.SignatureBuilder

```

:::warning Attention

Please make sure that the Overseer and FL servers ports are accessible by all participating sites.

:::

As you can see, developers will be able to add / modify the builders to streamline the project. 


## Provision in Dashboard

NVIDIA FLARE developed a Dashboard Web UI to set up the FL project easily. 
