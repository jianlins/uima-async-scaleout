/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.ee.test;

import static org.junit.Assert.*;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSSecurityException;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.usage.SystemUsage;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for UIMA AS with direct embedded ActiveMQ authentication
 */
public class EmbeddedAuthenticationTest {
  private static final Class<?> CLASS_NAME = EmbeddedAuthenticationTest.class;
  
  private static final String USERNAME = "uimauser";
  private static final String PASSWORD = "uimapass";
  private static final String WRONG_PASSWORD = "wrongpass";
  
  private static final String BROKER_URL = "tcp://localhost:61616";
  private static final String AUTH_BROKER_URL_FORMAT = BROKER_URL + "?jms.userName=%s&jms.password=%s";
  
  private static boolean brokerStarted = false;
  private static BrokerService secureBroker;
  private static TransportConnector secureConnector;
  private static CountDownLatch shutdownLatch = new CountDownLatch(1);
  
  private boolean authenticationFailureOccurred = false;
  private CountDownLatch authFailureLatch = new CountDownLatch(1);
  private final String primitiveDescriptor = "Deploy_NoOpAnnotator.xml";
  private String descriptorPath;
  
  /**
   * Set up method that starts the broker with authentication
   */
  @BeforeClass
  public static void startAuthenticatedBroker() throws Exception {
    try {
      System.out.println("Starting ActiveMQ broker with authentication...");
      
      // Create and configure a broker with authentication
      BrokerService broker = new BrokerService();
      broker.setBrokerName("testAuthBroker");
      broker.setPersistent(false);
      broker.setUseJmx(false);
      
      // Configure memory settings to avoid excessive memory usage
      SystemUsage systemUsage = broker.getSystemUsage();
      systemUsage.getMemoryUsage().setLimit(64 * 1024 * 1024); // 64 MB
      systemUsage.getTempUsage().setLimit(100 * 1024 * 1024); // 100 MB
      systemUsage.getStoreUsage().setLimit(100 * 1024 * 1024); // 100 MB
      
      // Configure authentication with SimpleAuthenticationPlugin
      List<AuthenticationUser> users = new ArrayList<>();
      users.add(new AuthenticationUser(USERNAME, PASSWORD, "users,admins"));
      
      SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);
      authenticationPlugin.setAnonymousAccessAllowed(false);
      
      broker.setPlugins(new BrokerPlugin[] { authenticationPlugin });
      
      // Add a transport connector
      TransportConnector connector = broker.addConnector("tcp://localhost:61616");
      
      // Start the broker
      broker.start();
      secureBroker = broker;
      secureConnector = connector;
      
      System.out.println("ActiveMQ broker started successfully with authentication at " + connector.getConnectUri());
      
      brokerStarted = true;
      
      // Add a shutdown hook to stop the broker when JVM exits
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          stopBroker();
          shutdownLatch.countDown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }));
    } catch (Exception e) {
      System.err.println("Error starting authenticated broker: " + e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }
  
  /**
   * Method to stop the broker after tests
   */
  @AfterClass
  public static void stopAuthenticatedBroker() throws Exception {
    stopBroker();
    shutdownLatch.await(15, TimeUnit.SECONDS);
  }
  
  private static void stopBroker() throws Exception {
    if (secureBroker != null) {
      System.out.println("Stopping ActiveMQ broker...");
      secureBroker.stop();
      secureBroker.waitUntilStopped();
      System.out.println("ActiveMQ broker stopped.");
    }
  }
  
  /**
   * Initialize test resources
   */
  public void setUp() throws Exception {
    // Initialize descriptor path
    URL descUrl = this.getClass().getClassLoader().getResource(primitiveDescriptor);
    if (descUrl != null) {
      descriptorPath = descUrl.getPath();
    } else {
      descriptorPath = "../examples/descriptors/deploy/" + primitiveDescriptor;
    }
  }
  
  /**
   * Helper method for deploying a service
   */
  private String deployService(BaseUIMAAsynchronousEngine_impl engine, String descriptor) throws Exception {
    return engine.deploy(descriptor, new HashMap<>());
  }
  
  /**
   * Helper method for initializing a client
   */
  private void initialize(BaseUIMAAsynchronousEngine_impl engine, Map<String, Object> context) throws Exception {
    engine.initialize(context);
  }
  
  /**
   * Test successful authentication to ActiveMQ broker
   */
  @Test
  public void testSuccessfulAuthentication() throws Exception {
    // Skip test if broker is not running
    if (!brokerStarted) {
      System.out.println("Skipping testSuccessfulAuthentication because authenticated broker is not running");
      return;
    }
    
    setUp(); // Initialize test resources
    
    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    
    try {
      // Create application context with authentication credentials in the URL
      Map<String, Object> appCtx = new HashMap<>();
      String authenticatedUrl = String.format(AUTH_BROKER_URL_FORMAT, USERNAME, PASSWORD);
      appCtx.put(UimaAsynchronousEngine.ServerUri, authenticatedUrl);
      appCtx.put(UimaAsynchronousEngine.ENDPOINT, "NoOpAnnotatorQueue");
      appCtx.put(UimaAsynchronousEngine.CasPoolSize, 2);
      
      // Deploy service first on the secure broker
      String containerId = deployService(uimaAsEngine, descriptorPath);
      
      try {
        // Initialize the client
        initialize(uimaAsEngine, appCtx);
        
        // Get a CAS and send it to service
        try {
          CAS cas = uimaAsEngine.getCAS();
          cas.setDocumentText("This is a test");
          uimaAsEngine.sendCAS(cas);
          
          // Clean up
          uimaAsEngine.collectionProcessingComplete();
          
          // Success - no authentication errors
          System.out.println("Successfully authenticated to broker with URL: " + authenticatedUrl);
        } finally {
          uimaAsEngine.stop();
        }
        
        // Verify no authentication errors occurred
        assertFalse("Authentication failure occurred when it should not have", authenticationFailureOccurred);
      } catch (Exception e) {
        System.out.println("Error in testSuccessfulAuthentication: " + e.getMessage());
        e.printStackTrace();
        fail("Unexpected exception: " + e.getMessage());
      }
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "testSuccessfulAuthentication", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
      fail("Unexpected exception: " + e.getMessage());
    }
  }
  
  /**
   * Test failed authentication to ActiveMQ broker with wrong password
   */
  @Test
  public void testFailedAuthentication() throws Exception {
    // Skip test if broker is not running
    if (!brokerStarted) {
      System.out.println("Skipping testFailedAuthentication because authenticated broker is not running");
      return;
    }
    
    setUp(); // Initialize test resources
    
    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    
    try {
      // Reset authentication failure flag
      authenticationFailureOccurred = false;
      authFailureLatch = new CountDownLatch(1);
      
      // Set up a special listener to detect authentication failure
      UimaAsBaseCallbackListener authListener = new UimaAsBaseCallbackListener() {
        @Override
        public void initializationComplete(EntityProcessStatus aStatus) {
          if (aStatus != null && aStatus.isException()) {
            for (Exception ex : aStatus.getExceptions()) {
              if (ex.getCause() instanceof JMSSecurityException || ex instanceof JMSSecurityException) {
                System.out.println("Authentication failure detected: " + ex.getMessage());
                authenticationFailureOccurred = true;
                authFailureLatch.countDown();
              }
            }
          }
        }
      };
      
      uimaAsEngine.addStatusCallbackListener(authListener);
      
      // Create application context with wrong authentication credentials in the URL
      Map<String, Object> appCtx = new HashMap<>();
      String authenticatedUrl = String.format(AUTH_BROKER_URL_FORMAT, USERNAME, WRONG_PASSWORD);
      appCtx.put(UimaAsynchronousEngine.ServerUri, authenticatedUrl);
      appCtx.put(UimaAsynchronousEngine.ENDPOINT, "NoOpAnnotatorQueue");
      appCtx.put(UimaAsynchronousEngine.CasPoolSize, 2);
      
      try {
        // Deploy service first on the secure broker
        try {
          String containerId = deployService(uimaAsEngine, descriptorPath);
        } catch (Exception e) {
          // Ignore deployment errors, we're testing authentication
          System.out.println("Expected deployment error: " + e.getMessage());
        }
        
        // This should fail due to wrong credentials
        initialize(uimaAsEngine, appCtx);
        
        // If we get here, wait a bit to see if the failure is detected asynchronously
        authFailureLatch.await(5, TimeUnit.SECONDS);
      } catch (ResourceInitializationException e) {
        // Expected exception - check if it's related to authentication
        if (e.getCause() != null && e.getCause().getCause() instanceof JMSSecurityException) {
          System.out.println("Authentication failure exception caught as expected: " + e.getMessage());
          authenticationFailureOccurred = true;
        } else {
          throw e; // Unexpected exception type
        }
      } finally {
        // Clean up
        try {
          uimaAsEngine.stop();
        } catch (Exception ignored) {}
      }
      
      // Verify authentication error occurred
      assertTrue("Authentication failure did not occur when it should have", authenticationFailureOccurred);
      
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "testFailedAuthentication", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
      throw e;
    }
  }
  
  /**
   * Test authentication with no credentials provided
   */
  @Test
  public void testNoCredentialsProvided() throws Exception {
    // Skip test if broker is not running
    if (!brokerStarted) {
      System.out.println("Skipping testNoCredentialsProvided because authenticated broker is not running");
      return;
    }
    
    setUp(); // Initialize test resources
    
    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    
    try {
      // Reset authentication failure flag
      authenticationFailureOccurred = false;
      authFailureLatch = new CountDownLatch(1);
      
      // Set up a special listener to detect authentication failure
      UimaAsBaseCallbackListener authListener = new UimaAsBaseCallbackListener() {
        @Override
        public void initializationComplete(EntityProcessStatus aStatus) {
          if (aStatus != null && aStatus.isException()) {
            for (Exception ex : aStatus.getExceptions()) {
              if (ex.getCause() instanceof JMSSecurityException || ex instanceof JMSSecurityException) {
                System.out.println("Authentication failure detected: " + ex.getMessage());
                authenticationFailureOccurred = true;
                authFailureLatch.countDown();
              }
            }
          }
        }
      };
      
      uimaAsEngine.addStatusCallbackListener(authListener);
      
      // Create application context with no authentication credentials
      Map<String, Object> appCtx = new HashMap<>();
      appCtx.put(UimaAsynchronousEngine.ServerUri, BROKER_URL);
      appCtx.put(UimaAsynchronousEngine.ENDPOINT, "NoOpAnnotatorQueue");
      appCtx.put(UimaAsynchronousEngine.CasPoolSize, 2);
      
      try {
        // Deploy service first on the secure broker
        try {
          String containerId = deployService(uimaAsEngine, descriptorPath);
        } catch (Exception e) {
          // Ignore deployment errors, we're testing authentication
          System.out.println("Expected deployment error: " + e.getMessage());
        }
        
        // This should fail due to missing credentials
        initialize(uimaAsEngine, appCtx);
        
        // If we get here, wait a bit to see if the failure is detected asynchronously
        authFailureLatch.await(5, TimeUnit.SECONDS);
      } catch (ResourceInitializationException e) {
        // Expected exception - check if it's related to authentication
        if (e.getCause() != null && e.getCause().getCause() instanceof JMSSecurityException) {
          System.out.println("Authentication failure exception caught as expected: " + e.getMessage());
          authenticationFailureOccurred = true;
        } else {
          throw e; // Unexpected exception type
        }
      } finally {
        // Clean up
        try {
          uimaAsEngine.stop();
        } catch (Exception ignored) {}
      }
      
      // Verify authentication error occurred
      assertTrue("Authentication failure did not occur when credentials were missing", authenticationFailureOccurred);
      
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "testNoCredentialsProvided", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
      throw e;
    }
  }
  
  /**
   * Alternative method of testing with credentials as broker properties
   */
  @Test
  public void testPropertiesAuthentication() throws Exception {
    // Skip test if broker is not running
    if (!brokerStarted) {
      System.out.println("Skipping testPropertiesAuthentication because authenticated broker is not running");
      return;
    }
    
    setUp(); // Initialize test resources
    
    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    
    try {
      // Create application context with authentication credentials as properties
      // This is an alternative to including them in the URL
      Map<String, Object> appCtx = new HashMap<>();
      appCtx.put(UimaAsynchronousEngine.ServerUri, BROKER_URL);
      appCtx.put(UimaAsynchronousEngine.ENDPOINT, "NoOpAnnotatorQueue");
      appCtx.put(UimaAsynchronousEngine.CasPoolSize, 2);
      appCtx.put(UimaAsynchronousEngine.userName, USERNAME);
      appCtx.put(UimaAsynchronousEngine.password, PASSWORD);
      
      // Deploy service first on the secure broker
      String containerId = deployService(uimaAsEngine, descriptorPath);
      
      try {
        // Initialize the client
        initialize(uimaAsEngine, appCtx);
        
        // Get a CAS and send it to service
        try {
          CAS cas = uimaAsEngine.getCAS();
          cas.setDocumentText("This is a test");
          uimaAsEngine.sendCAS(cas);
          
          // Clean up
          uimaAsEngine.collectionProcessingComplete();
          
          // Success - no authentication errors
          System.out.println("Successfully authenticated to broker with properties authentication");
        } finally {
          uimaAsEngine.stop();
        }
        
        // Verify no authentication errors occurred
        assertFalse("Authentication failure occurred when it should not have", authenticationFailureOccurred);
      } catch (Exception e) {
        System.out.println("Error in testPropertiesAuthentication: " + e.getMessage());
        e.printStackTrace();
        fail("Unexpected exception: " + e.getMessage());
      }
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "testPropertiesAuthentication", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
      fail("Unexpected exception: " + e.getMessage());
    }
  }
}
