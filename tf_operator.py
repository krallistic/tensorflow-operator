from kubernetes import client, config, watch
import structlog
log = structlog.get_logger()

#config.load_kube_config()


#api_instance = client.CoreV1Api()



#w = watch.Watch()
#for event in w.stream(api_instance.list_pod_for_all_namespaces):
#    print("Event: %s %s %s" % (event['type'],event['object'].kind, event['object'].metadata.name))

def create_crd():
    log.msg("Creating CustomResourceDefinition")
    api_instance = client.CustomObjectsApi()
    group = 'krallistic.github.com' # str | The custom resource's group name
    version = 'v1alpha1' # str | The custom resource's version
    plural = 'tensorflows' # str | The custom resource's plural name. For TPRs this would be lowercase plural kind.
    body = "NULL" # object | The JSON schema of the Resource to create.
    pretty = 'true' # str | If 'true', then the output is pretty printed. (optional)
    api_response = api_instance.list_cluster_custom_object(group, version, plural, pretty=pretty, watch=True)
    log.msg(str(api_response))

def callback_function(response):
    log.msg("Callback list tf called")
    print(response)


def main():
    log.msg("Starting Tensorflow Operator")

    config.load_kube_config()
    log.msg("Loaded Kubernetes Config")
    #create_crd()
    api_instance = client.CustomObjectsApi()
    group = 'krallistic.github.com' # str | The custom resource's group name
    version = 'v1alpha1' # str | The custom resource's version
    plural = 'tensorflows' # str | The custom resource's plural name. For TPRs this would be lowercase plural kind.
    body = "NULL" # object | The JSON schema of the Resource to create.
    pretty = 'true' # str | If 'true', then the output is pretty printed. (optional)

    w = watch.Watch()
    #events = api_instance.list_cluster_custom_object(group, version, plural, pretty=True, watch=True, callback=callback_function)
    log.msg("Start watching kubernetes API")
        
    for event in w.stream(api_instance.list_cluster_custom_object, group, version, plural, pretty=True, watch=True):
        #for event in events:
        log.msg("Got event from k8s API")
        event_type = event['type']
        event_object = event['object']
        log.msg(str("Event Type: " +  str(event_type) +  " Event body " +  str(event_object)))
        print(event)
        if event_type == "ADDED":
            create_tensorflow_training(event_object)
        elif event_type == "DELETED":
            delete_tensorflow_training(event_object)
        elif event_type == "MODIFIED":
            update_tensorflow_training(event_object)
        else:
            log.msg("Unknown EVENT type")
        #print("Event: %s %s" % (event['type'], event['object']))
        if False:
            w.stop()

    log.msg("Watch closed")


def build_nodes_spec_string(spec, type):
    namespace =  spec['metadata']['namespace']
    name = spec['metadata']['name']
    
    if type == "worker":
        count = int(spec['spec']['worker_nodes'])
    elif type == "ps":
        count = int(spec['spec']['ps_nodes'])
    else:
        print("Unkown type")

    spec_string = ""
    for i in range(count):
        spec_string += str(type) + "-" + str(i) + "." + str(namespace) + ".cluster.local:2222"
    return spec_string


def create_tensorflow_training(spec):
    log.msg("Creating Tensorflow training")
    worker_spec = build_nodes_spec_string(spec, "worker")
    ps_spec = build_nodes_spec_string(spec, "ps")
    for i in range(int(spec['spec']['worker_nodes'])):
        print("Creating worker-" + str(i))
        generate_service_spec(spec, "worker", i )
        generate_job_spec(spec, "worker", i)

        
    
    for i in range(int(spec['spec']['ps_nodes'])):
        print("Creating worker-" + str(i))
        generate_job_spec(spec, "ps", i)
        generate_service_spec(spec, "ps", i )



    print("Worker Spec: ", worker_spec, ps_spec)

def generate_name(spec, node_type, node_index):
    return spec['metadata']['name'] + "-" + node_type + "-" + str(node_index)

def generate_labels(spec, node_type, node_index):
    return {
            "app": "tensorflow",
            "node-type": str(node_type),
            "task-index": str(node_index),
            "name" : spec['metadata']['name']
            }

def generate_service_spec(spec, node_type, node_index):
    log.msg("Generating Service Object for " + str(node_type) + " " + str(node_index))
    v1 = client.CoreV1Api()
    service = client.V1Service()
    service.kind = "Service"
    service.api_version = "v1"
    service.metadata = client.V1ObjectMeta(name=generate_name(spec, node_type, node_index), 
                                            labels=generate_labels(spec, node_type, node_index))
    service_spec = client.V1ServiceSpec()
    port = client.V1ServicePort(port=2222)
    service_spec.ports = [ port ] 

    service_spec.selector = generate_labels(spec, node_type, node_index)
    service.spec = service_spec
    v1.create_namespaced_service(namespace=spec['metadata']['namespace'], body=service)

def generate_arguments(spec, node_type, node_index):
    return [
            "--job-name=" + node_type,
            "--task-index=" + str(node_index),
            "--ps-tasks=" + build_nodes_spec_string(spec, "ps"),
            "--worker-tasks=" + build_nodes_spec_string(spec, "worker"),
            #TODO 
        ] + spec['spec']['additional_args']

def generate_job_spec(spec, node_type, node_index):
    log.msg("Generating Worker Spec")
    extension = client.ExtensionsV1beta1Api()
    deployment = client.ExtensionsV1beta1Deployment()
    deployment.api_version = "extensions/v1beta1"
    deployment.kind = "Deployment"
    deployment.metadata = client.V1ObjectMeta(labels=generate_labels(spec, node_type, node_index),
                                                name=generate_name(spec, node_type, node_index))
    deployment_spec = client.ExtensionsV1beta1DeploymentSpec()
    deployment_spec.replicas = 1

    deployment_spec.template = client.V1PodTemplateSpec()
    deployment_spec.template.metadata = client.V1ObjectMeta(labels=generate_labels(spec, node_type, node_index))
    deployment_spec.template.spec = client.V1PodSpec()

    container = client.V1Container()
    container.name="tensorflow"
    container.image=spec['spec']['image']



    container.args = generate_arguments(spec, node_type, node_index)
    if spec['spec']['port']:
        port = spec['spec']['port']
    else:
       port = 2222 

    container.ports = [client.V1ContainerPort(container_port=port)]

    deployment_spec.template.spec.containers = [container]
    deployment.spec = deployment_spec
    print("Creating Deployment")
    extension.create_namespaced_deployment(namespace=spec['metadata']['namespace'], body=deployment)


def update_tensorflow_training(spec):
    log.msg("Update Training, delete existing first")
    delete_tensorflow_training(spec)
    create_tensorflow_training(spec)

def delete_tensorflow_training(spec):
    log.msg("Deleting Tensorflow Training")
    for i in range(int(spec['spec']['worker_nodes'])):
        print("Deleting worker-" + str(i))
        delete_stuff(spec, "worker", i )
 
    
    for i in range(int(spec['spec']['ps_nodes'])):
        print("Deleting PS-" + str(i))
        delete_stuff(spec, "ps", i)


def delete_stuff(spec, node_type, node_index):
    extension = client.ExtensionsV1beta1Api()
    deployment = client.ExtensionsV1beta1Deployment()
    deployment.api_version = "extensions/v1beta1"
    deployment.kind = "Deployment"
    deployment.metadata = client.V1ObjectMeta(labels=generate_labels(spec, node_type, node_index),
                                                name=generate_name(spec, node_type, node_index))
    extension.delete_namespaced_deployment(name=generate_name(spec, node_type, node_index), namespace=spec['metadata']['namespace'], body=client.V1DeleteOptions(propagation_policy="Foreground", grace_period_seconds=5))
    
    v1 = client.CoreV1Api()

    service = client.V1Service()
    service.kind = "Service"
    service.api_version = "v1"
    service.metadata = client.V1ObjectMeta(name=generate_name(spec, node_type, node_index), 
                                            labels=generate_labels(spec, node_type, node_index))

    v1.delete_namespaced_service(name=generate_name(spec, node_type, node_index), namespace=spec['metadata']['namespace'], body=client.V1DeleteOptions(propagation_policy="Foreground", grace_period_seconds=5))


if __name__ == '__main__':
    main()
    