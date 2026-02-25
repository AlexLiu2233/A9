Group ID: G20
Verification Code: 5409645920
Used Run Command: java -Xmx64m -jar A7.jar <add more, this isn't done>
Brief Description: A consistent hashring was built in order to handle mapping the nodes to a ring-space. It uses a TreeMap as an underlying structure, and is secured with a ReentrantReadWriteLock. Since reads are likely to be far more common than writes, this was used to ensure that multiple simultaneous reads could be handled, while still ensuring correctness for writes. For communication between nodes, a gossip-style service was used.