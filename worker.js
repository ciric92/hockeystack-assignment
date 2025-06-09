const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ accessToken: '' });
const propertyPrefix = 'hubspot__';
let expirationDate;

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

/***
 * Exponential backoff function
 * @param {Function} fn - The function to execute
 * @param {number} maxRetries - Maximum number of retries
 * @param {number} delay - Initial delay in milliseconds
 */
const backoff = async (fn, maxRetries = 4, delay = 5000) => {
  let tryCount = 0;
  while (true) {
    try {
      return await fn();
    } catch (e) {
      tryCount++;
      if (tryCount > maxRetries) {
        console.error(`Failed to execute function after ${maxRetries} retries.`, e);
        throw e;
      }
      console.warn(`Retrying function execution (${tryCount}/${maxRetries}) after error:`, e.message);
      if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);
      await new Promise((resolve, _) => setTimeout(resolve, delay * Math.pow(2, tryCount)));
    }
  }
}

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit,
      after: offsetObject.after
    };

    const searchResult = await backoff(async () => {
      return await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
    });

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    console.log('fetch company batch');

    data.forEach(company => {
      if (!company.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

      q.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.companies = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit,
      after: offsetObject.after
    };

    const searchResult = await backoff(async () => {
      return await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
    });
    
    const data = searchResult.results || [];

    console.log('fetch contact batch');

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: { inputs: contactsToAssociate.map(contactId => ({ id: contactId })) }
    })).json())?.results || [];

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
      if (a.from) {
        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    data.forEach(contact => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      q.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);

  return true;
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

/***
 * processMeetings fethces meetings from HubSpot API and processes them as actions.
 * @param {Object} domain - The domain object containing integration details
 * @param {string} hubId - The HubSpot account ID
 * @param {queue} q - The queue to push actions to
 */
const processMeetings = async(domain, hubId, q) => {

  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const now = new Date();
  
  const offsetObject = {};
  const limit = 100;
  
  while (true) {
    const lastPulledDate = new Date(account.lastPulledDates.meetings);
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'hs_lastmodifieddate');

    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: ['hs_meeting_title', 'hs_meeting_start_time', 'hs_meeting_end_time'],
      limit,
      after: offsetObject.after
    };

    const searchResults = await backoff(async () => {
      return await hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject);
    });
    const meetings = searchResults.results || [];

    // Lookup the contacts that attended the meetings
    const attendingContacts = await backoff(async () => {
      const response = await hubspotClient.apiRequest({
        method: 'POST',
        path: '/crm/v3/associations/MEETINGS/CONTACTS/batch/read',
        body: {
          inputs:  meetings.map(meeting => ({ id: meeting.id }))
        }
      });
      return await response.json();
    });

    const uniqueContactIds = new Set();
    const contactLookup = attendingContacts.results.reduce((acc, item) => {
      if (item.from && item.to && item.to.length > 0) {
        acc[item.from.id] = item.to.map(contact => contact.id);
        item.to.forEach(contact => uniqueContactIds.add(contact.id));
      }
      return acc;
    }, {});

    const attendingEmailsResponse = await backoff(async () => {
      return await hubspotClient.crm.contacts.batchApi.read({
        inputs: Array.from(uniqueContactIds).map(id => ({id})), 
        properties: ['email']
      });
    }); 
    const attendingEmails = attendingEmailsResponse.results.reduce((acc, contact) => {
      if (contact.properties && contact.properties.email) {
        acc[contact.id] = contact.properties.email;
      }
      return acc;
    }, {});

    offsetObject.after = parseInt(searchResults.paging?.next?.after);

    console.log('fetch meeting batch');

    meetings.forEach(meeting => {
      if (!meeting.properties) return;

      const contacts = contactLookup[meeting.id] || [];
      const contactEmails = contacts.map(contactId => attendingEmails[contactId]).filter(email => email);

      const actionTemplate = {
        includeInAnalytics: 0,
        meetingProperties: {
          meeting_id: meeting.id,
          meeting_title: meeting.properties.hs_meeting_title,
          meeting_start_time: new Date(meeting.properties.hs_meeting_start_time),
          meeting_end_time: new Date(meeting.properties.hs_meeting_end_time),
          meeting_created_at: new Date(meeting.createdAt),
          meeting_updated_at: new Date(meeting.updatedAt),
          meeting_contacts: contactEmails, 
        }
      };

      // Update and creation timestamps differ in HubSpot API for newly created resources.
      // Because of that we are checking lastPulledDate against createdAt because we assume that 
      // the meeting is created if we see it for the first time:
      // - First pull (no lastPulledDate) => meeting is created
      // - Any pull after that => if meeting.createdAt is after lastPulledDate (meaning it's in current page) then it's created, otherwise it's updated
      const isCreated = !lastPulledDate || (new Date(meeting.createdAt) > lastPulledDate);

      // Saving lastPulledDates for meetings to ensure the above isCreated logic works correctly between frames.
      account.lastPulledDates.meetings = now;

      q.push({
        actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
        actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
        ...actionTemplate
      });
    });


    if (!offsetObject?.after) {
      break;
    } else if (offsetObject?.after >= 9900) {
      // We've reached the maximum offset value for pagination
      // Resetting the offset object so that it starts from the lastPulledDate
      offsetObject = {};
    }
  }

  await saveDomain(domain);

  return true;
}

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);
      console.log('process contacts');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('process companies');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    }

    try {
      await processMeetings(domain, account.hubId, q);
      console.log('process meetings');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processMeetings', hubId: account.hubId } });
    }


    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    console.log('finish processing account');
  }

  process.exit();
};

module.exports = pullDataFromHubspot;
