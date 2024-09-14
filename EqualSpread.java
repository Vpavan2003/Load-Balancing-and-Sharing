import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

import java.text.DecimalFormat;
import java.util.*;

public class EqualSpread {

    private static List<Vm> vmlist = new ArrayList<>();
    private static List<Host> hostList = new ArrayList<>();
    private static List<DatacenterBroker> brokers = new ArrayList<>();

    // Define Prometheus metrics
    static final Counter cloudletCounter = Counter.build()
            .name("cloudlets_total")
            .help("Total number of cloudlets processed.")
            .register();

    static final Counter vmMigrationCounter = Counter.build()
            .name("vm_migrations_total")
            .help("Total number of VM migrations.")
            .register();

    static final Histogram responseTimeHistogram = Histogram.build()
            .name("cloudlet_response_time_seconds")
            .help("Response time of cloudlets.")
            .register();

    static final Gauge cpuUtilizationGauge = Gauge.build()
            .name("host_cpu_utilization")
            .help("CPU utilization of hosts.")
            .labelNames("host_id")
            .register();

    static final Gauge memoryUtilizationGauge = Gauge.build()
            .name("host_memory_utilization")
            .help("Memory utilization of hosts.")
            .labelNames("host_id")
            .register();

    static final Counter requestsTotal = Counter.build()
            .name("http_requests_total")
            .help("Total incoming HTTP requests.")
            .register();

    static final Histogram responseTime = Histogram.build()
            .name("http_response_time_seconds")
            .help("Response time in seconds.")
            .register();

    static final Gauge backendLoad = Gauge.build()
            .name("backend_instance_load")
            .help("Load on backend instances.")
            .labelNames("instance")
            .register();

    static final Gauge backendHealth = Gauge.build()
            .name("backend_instance_health_check")
            .help("Health check status of backend instances.")
            .labelNames("instance")
            .register();

    public static void main(String[] args) {
        try {
            // Initialize the Prometheus metrics exporter
            HTTPServer server = new HTTPServer(1237);  // Exposes metrics on http://localhost:1236/metrics
            DefaultExports.initialize();

            // Initialize CloudSim and run the simulation as before
            int num_user = 1; // number of cloud users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;

            CloudSim.init(num_user, calendar, trace_flag);

            // Create Datacenter
            Datacenter datacenter = createDatacenter("Datacenter_0");

            // Create Brokers
            int brokerCount = 2; // Number of brokers for redundancy
            for (int i = 0; i < brokerCount; i++) {
                DatacenterBroker broker = createBroker();
                if (broker != null) {
                    brokers.add(broker);
                }
            }

            if (brokers.isEmpty()) {
                System.err.println("No brokers created, exiting...");
                return;
            }

            // Use the first broker for initial submissions
            DatacenterBroker primaryBroker = brokers.get(0);
            int brokerId = primaryBroker.getId();

            // Create VMs
            int vmid = 0;
            int mips = 1000;
            int ram = 2048; // VM memory (MB)
            long bw = 10000; // bandwidth
            long size = 100000; // image size (MB)
            String vmm = "Xen"; // VMM name

            for (int i = 0; i < 10; i++) {
                Vm vm = new Vm(vmid, brokerId, mips, 1, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
                vmlist.add(vm);
                vmid++;
            }

            primaryBroker.submitVmList(vmlist);

            // Create Cloudlets
            List<Cloudlet> cloudletList = new ArrayList<>();
            int id = 0;
            int pesNumber = 1;
            long length = 400000;
            long fileSize = 300;
            long outputSize = 300;
            UtilizationModel utilizationModel = new UtilizationModelFull();

            for (int i = 0; i < 20; i++) {
                Cloudlet cloudlet = new Cloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
                cloudlet.setUserId(brokerId);
                cloudletList.add(cloudlet);
                id++;
            }

            primaryBroker.submitCloudletList(cloudletList);

            // Implement Equal Spread Load Balancer
            EqualSpreadLoadBalancer eslb = new EqualSpreadLoadBalancer(primaryBroker, vmlist);
            eslb.balanceLoad(cloudletList);

            // Start simulation
            CloudSim.startSimulation();

            // Monitor and migrate VMs based on load
            monitorAndMigrate();

            // Stop simulation
            List<Cloudlet> newList = primaryBroker.getCloudletReceivedList();
            CloudSim.stopSimulation();

            // Print results and update response time histogram
            printCloudletList(newList);

            for (Cloudlet cloudlet : cloudletList) {
                responseTimeHistogram.observe(cloudlet.getFinishTime() - cloudlet.getExecStartTime());
                cloudletCounter.inc();  // Increment cloudlet counter
            }

            // Stop Prometheus HTTP server on application exit
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop();
            }));

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unwanted errors happened");
        }
    }

    private static Datacenter createDatacenter(String name) {
        int mips = 1000;
        int ram = 16384; // host memory (MB)
        long storage = 1000000; // host storage
        int bw = 100000;

        for (int i = 0; i < 10; i++) {
            List<Pe> peList = new ArrayList<>();
            peList.add(new Pe(0, new PeProvisionerSimple(mips)));

            hostList.add(new Host(
                    i,
                    new RamProvisionerSimple(ram),
                    new BwProvisionerSimple(bw),
                    storage,
                    peList,
                    new VmSchedulerTimeShared(peList)
            ));
        }

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;

        LinkedList<Storage> storageList = new LinkedList<>();

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        Datacenter datacenter = null;
        try {
            datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

    private static DatacenterBroker createBroker() {
        DatacenterBroker broker = null;
        try {
            broker = new DatacenterBroker("Broker");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return broker;
    }

    private static void printCloudletList(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        System.out.println();
        System.out.println("========== OUTPUT ==========");
        System.out.println("Cloudlet ID" + indent + "STATUS" + indent +
                "Data center ID" + indent + "VM ID" + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            System.out.print(indent + cloudlet.getCloudletId() + indent + indent);

            if (cloudlet.getStatus() == Cloudlet.SUCCESS) {
                System.out.println("SUCCESS" + indent + indent + cloudlet.getResourceId() + indent + indent + cloudlet.getVmId() +
                        indent + indent + dft.format(cloudlet.getActualCPUTime()) +
                        indent + indent + dft.format(cloudlet.getExecStartTime()) + indent + indent + dft.format(cloudlet.getFinishTime()));
            }
        }
    }

    static class EqualSpreadLoadBalancer {

        private DatacenterBroker broker;
        private List<Vm> vmlist;

        public EqualSpreadLoadBalancer(DatacenterBroker broker, List<Vm> vmlist) {
            this.broker = broker;
            this.vmlist = vmlist;
        }

        public void balanceLoad(List<Cloudlet> cloudletList) {
            int vmIndex = 0;

            // Assign each cloudlet to VMs in a round-robin fashion to ensure equal spread
            for (Cloudlet cloudlet : cloudletList) {
                Vm vm = vmlist.get(vmIndex);
                cloudlet.setVmId(vm.getId());

                // Move to the next VM in the list, wrapping around if necessary
                vmIndex = (vmIndex + 1) % vmlist.size();
            }
        }
    }

    private static void monitorAndMigrate() {
        List<Vm> vmsToMigrate = new ArrayList<>();
        for (Host host : hostList) {
            double hostCpuUtilization = getHostUtilization(host);
            cpuUtilizationGauge.labels(String.valueOf(host.getId())).set(hostCpuUtilization);

            if (hostCpuUtilization > 0.75) { // Threshold utilization
                for (Vm vm : host.getVmList()) {
                    if (vm.getMips() < host.getVmScheduler().getAvailableMips()) {
                        vmsToMigrate.add(vm);
                    }
                }
            }
            memoryUtilizationGauge.labels(String.valueOf(host.getId())).set(getMemoryUtilization(host));
        }
        if (!vmsToMigrate.isEmpty()) {
            for (Vm vm : vmsToMigrate) {
                Host newHost = findHostForVm(vm);
                if (newHost != null) {
                    migrateVm(vm, newHost);
                    vmMigrationCounter.inc(); // Increment VM migration counter
                }
            }
        }
    }

    private static double getHostUtilization(Host host) {
        double utilization = 0;
        for (Pe pe : host.getPeList()) {
            utilization += pe.getPeProvisioner().getUtilization();
        }
        return utilization / host.getPeList().size();
    }

    private static double getMemoryUtilization(Host host) {
        double usedMemory = host.getRamProvisioner().getUsedRam();
        double totalMemory = host.getRamProvisioner().getRam();
        return usedMemory / totalMemory;
    }

    private static Host findHostForVm(Vm vm) {
        for (Host host : hostList) {
            if (host.getVmScheduler().getAvailableMips() > vm.getMips()) {
                return host;
            }
        }
        return null;
    }

    private static void migrateVm(Vm vm, Host newHost) {
        Host oldHost = vm.getHost();
        oldHost.vmDestroy(vm);
        newHost.vmCreate(vm);
        vm.setHost(newHost);
        System.out.println("VM " + vm.getId() + " migrated from Host " + oldHost.getId() + " to Host " + newHost.getId());
    }
}
