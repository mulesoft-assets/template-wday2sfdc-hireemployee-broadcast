
# Anypoint Template: Workday to Salesforce Hire Employee Broadcast

<!-- Header (start) -->

<!-- Header (end) -->

# License Agreement
This template is subject to the conditions of the <a href="https://s3.amazonaws.com/templates-examples/AnypointTemplateLicense.pdf">MuleSoft License Agreement</a>. Review the terms of the license before downloading and using this template. You can use this template for free with the Mule Enterprise Edition, CloudHub, or as a trial in Anypoint Studio.
# Use Case
<!-- Use Case (start) -->
This template serves as a foundation for setting an online sync of Employee Hires from Workday to Salesforce Users.

Every time there is a new Employee Hire or a change in an already existing one, the integration will poll for them in the Workday source instance and it will be responsible for creating/updating the User in Salesforce with the specified PermissionSet.

Requirements have been set not only to be used as examples, but also to establish a starting point to adapt your integration to your requirements.

As implemented, this template leverages the batch module. The batch job is divided in *Process* and *On Complete* stages.

The template retrieves all Employee Hires that have changed since last poll and if there are any, triggers the Batch process. During the *Process* stage, in the first step the Employees are matched by email with the existing Users in Salesforce. In the second step the users are upserted to Salesforce using a Batch Aggregator and updated with the specified PermissionSet.

Finally during the *On Complete* stage the template logs output statistics data into the console.
<!-- Use Case (end) -->

# Considerations
<!-- Default Considerations (start) -->

<!-- Default Considerations (end) -->

<!-- Considerations (start) -->
To make this template run, there are certain preconditions that must be considered. All of them deal with the preparations in both source and destination systems, that must be made in order for all to run smoothly. **Failing to do so could lead to unexpected behavior of the template.**

1. **Users cannot be deleted in Salesforce:** For now, the only thing to do regarding users removal is disabling/deactivating them, but this won't make the username available for a new user.
2. **Each user needs to be associated to a Profile:** Salesforce's profiles are what define the permissions the user will have for manipulating data and other users. Each Salesforce account has its own profiles.
3. **Working with sandboxes for the same account**: Although each sandbox should be a completely different environment, Usernames cannot be repeated in different sandboxes, i.e. if you have a user with username *bob.dylan* in *sandbox A*, you will not be able to create another user with username *bob.dylan* in *sandbox B*.
4. **Workday email uniqueness**: The email can be repeated for two or more accounts (or missing).
<!-- Considerations (end) -->



## Salesforce Considerations

Here's what you need to know about Salesforce to get this template to work:

- Where can I check that the field configuration for my Salesforce instance is the right one? See: <a href="https://help.salesforce.com/HTViewHelpDoc?id=checking_field_accessibility_for_a_particular_field.htm&language=en_US">Salesforce: Checking Field Accessibility for a Particular Field</a>.
- Can I modify the Field Access Settings? How? See: <a href="https://help.salesforce.com/HTViewHelpDoc?id=modifying_field_access_settings.htm&language=en_US">Salesforce: Modifying Field Access Settings</a>.


### As a Data Destination

There are no considerations with using Salesforce as a data destination.


## Workday Considerations

### As a Data Source

There are no considerations with using Workday as a data origin.







# Run it!
Simple steps to get this template running.
<!-- Run it (start) -->

<!-- Run it (end) -->

## Running On Premises
In this section we help you run this template on your computer.
<!-- Running on premise (start) -->

<!-- Running on premise (end) -->

### Where to Download Anypoint Studio and the Mule Runtime
If you are new to Mule, download this software:

+ [Download Anypoint Studio](https://www.mulesoft.com/platform/studio)
+ [Download Mule runtime](https://www.mulesoft.com/lp/dl/mule-esb-enterprise)

**Note:** Anypoint Studio requires JDK 8.
<!-- Where to download (start) -->

<!-- Where to download (end) -->

### Importing a Template into Studio
In Studio, click the Exchange X icon in the upper left of the taskbar, log in with your Anypoint Platform credentials, search for the template, and click Open.
<!-- Importing into Studio (start) -->

<!-- Importing into Studio (end) -->

### Running on Studio
After you import your template into Anypoint Studio, follow these steps to run it:

+ Locate the properties file `mule.dev.properties`, in src/main/resources.
+ Complete all the properties required as per the examples in the "Properties to Configure" section.
+ Right click the template project folder.
+ Hover your mouse over `Run as`.
+ Click `Mule Application (configure)`.
+ Inside the dialog, select Environment and set the variable `mule.env` to the value `dev`.
+ Click `Run`.
<!-- Running on Studio (start) -->

<!-- Running on Studio (end) -->

### Running on Mule Standalone
Update the properties in one of the property files, for example in mule.prod.properties, and run your app with a corresponding environment variable. In this example, use `mule.env=prod`.


## Running on CloudHub
When creating your application in CloudHub, go to Runtime Manager > Manage Application > Properties to set the environment variables listed in "Properties to Configure" as well as the mule.env value.
<!-- Running on Cloudhub (start) -->

<!-- Running on Cloudhub (end) -->

### Deploying a Template in CloudHub
In Studio, right click your project name in Package Explorer and select Anypoint Platform > Deploy on CloudHub.
<!-- Deploying on Cloudhub (start) -->

<!-- Deploying on Cloudhub (end) -->

## Properties to Configure
To use this template, configure properties such as credentials, configurations, etc.) in the properties file or in CloudHub from Runtime Manager > Manage Application > Properties. The sections that follow list example values.
### Application Configuration
<!-- Application Configuration (start) -->
+ page.size `200`

**Scheduler Configuration**
+ scheduler.frequency `60000`
+ scheduler.start.delay `1000`
+ watermark.default.expression `2016-12-13T03:00:59Z`

**Workday Connector Configuration**
+ wday.username `admin@workday`
+ wday.password `secret`
+ wday.tenant `tenant`
+ wday.host `impl-cc.workday.com`

**Salesforce Connector Configuration**
+ sfdc.username `user@company.com`
+ sfdc.password `secret`
+ sfdc.securityToken `1234fdkfdkso20kw2sd`

+ user.localeSidKey `en_US`
+ user.languageLocaleKey `en_US`
+ user.timeZoneSidKey `America/New_York`
+ user.emailEncodingKey `ISO-8859-1`

+ sfdc.user.profile.id `10000000002D4JWAA0`
+ sfdc.user.permission.id `10000000002D5JWAA0`
<!-- Application Configuration (end) -->

# API Calls
<!-- API Calls (start) -->
Salesforce imposes limits on the number of API calls that can be made. Therefore calculating this amount may be an important factor to consider. The template calls to the API can be calculated using the formula:

***1 + X + X / ${page.size}***

***X*** is the number of Users to be synchronized on each run.

Divide by ***${page.size}*** because by default, Users are gathered in groups of ${page.size} for each Upsert API Call in the aggregation step. Also consider that these calls are executed repeatedly every polling cycle.

For instance if 10 records are fetched from origin instance, then 12 API calls will be made (1 + 10 + 1).
<!-- API Calls (end) -->

# Customize It!
This brief guide provides a high level understanding of how this template is built and how you can change it according to your needs. As Mule applications are based on XML files, this page describes the XML files used with this template. More files are available such as test classes and Mule application files, but to keep it simple, we focus on these XML files:

* config.xml
* businessLogic.xml
* endpoints.xml
* errorHandling.xml<!-- Customize it (start) -->

<!-- Customize it (end) -->

## config.xml
<!-- Default Config XML (start) -->
This file provides the configuration for connectors and configuration properties. Only change this file to make core changes to the connector processing logic. Otherwise, all parameters that can be modified should instead be in a properties file, which is the recommended place to make changes.<!-- Default Config XML (end) -->

<!-- Config XML (start) -->

<!-- Config XML (end) -->

## businessLogic.xml
<!-- Default Business Logic XML (start) -->
Functional aspect of the template is implemented in this XML, directed by a batch job that will be responsible for creations/updates. The several message processors constitute four high level actions that fully implement the logic of this template:

1. Job execution is invoked from schedulerFlow (endpoints.xml) every time there is a new query executed asking for created/updated Workers.
2. During the *Process* stage, each Workers will be filtered depending on, if it has an existing matching User in the Salesforce. The match is done according to the email addresses.
3. The last step of the *Process* stage will group the Users and create/update them in Salesforce and upsert PermissionSet if it is not exist.
4. Finally during the *On Complete* stage the template logs output statistics data into the console.<!-- Default Business Logic XML (end) -->

<!-- Business Logic XML (start) -->

<!-- Business Logic XML (end) -->

## endpoints.xml
<!-- Default Endpoints XML (start) -->
This file contains the endpoints for triggering the template and for retrieving the objects that meet the defined criteria in a query. You can execute a batch job process with the query results.<!-- Default Endpoints XML (end) -->

<!-- Endpoints XML (start) -->

<!-- Endpoints XML (end) -->

## errorHandling.xml
<!-- Default Error Handling XML (start) -->
This file handles how your integration reacts depending on the different exceptions. This file provides error handling that is referenced by the main flow in the business logic.<!-- Default Error Handling XML (end) -->

<!-- Error Handling XML (start) -->

<!-- Error Handling XML (end) -->

<!-- Extras (start) -->

<!-- Extras (end) -->
