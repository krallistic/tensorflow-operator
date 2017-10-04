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

    v1 = client.CoreV1Api()
    count = 10
    w = watch.Watch()
    for event in w.stream(v1.list_namespace, _request_timeout=60):
        log.msg("Watching kubernetes API")
        print("Event: %s %s" % (event['type'], event['object'].metadata.name))
        count -= 1
        if not count:
            w.stop()


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
        spec_string += str(type) + "-" + str(i) + "." + str(namespace) + "cluster.local:2222"
    return spec_string

def build_ps_nodes_spec_string(spec):
    pass

def create_tensorflow_training(spec):
    log.msg("Creating Tensorflow training")
    worker_spec = build_nodes_spec_string(spec, "worker")
    ps_spec = build_nodes_spec_string(spec, "ps")

    print("Worker Spec: ", worker_spec, ps_spec)

def update_tensorflow_training(spec):
    log.msg("Update Training, delete existing first")
    delete_tensorflow_training(spec)
    create_tensorflow_training(spec)

def delete_tensorflow_training(spec):
    log.msg("Deleting Tensorflow Training")


if __name__ == '__main__':
    main()
    