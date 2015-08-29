/*eslint-disable */
var knex = require('knex')({
  client: 'mysql',
  connection: {
    host     : '127.0.0.1',
    user     : 'root',
    password : '',
    database : 'bmi'
  }
})

var _ = require('lodash')
var async = require('async')
var csv = require('ya-csv')


async.series([
  function updateExistingMasterAgenciesList(seriesCb) {
    console.log('updating existing master agencies list...')
    //grab all rows from agencies_global_master
    knex.from('agencies_global_master').then(function(agenciesFromMaster) {

      //foreach agency from agencies_global_master...
      async.eachSeries(agenciesFromMaster, function(agencyFromMaster, eachSeriesCb) {

        //lookup the corresponding company from `company`
        var lookupCompanyObj = knex.from('company')

        lookupCompanyObj.where('name', agencyFromMaster.name)

        if (agencyFromMaster.other_name) {
          lookupCompanyObj.orWhere('name', agencyFromMaster.other_name)
        }
        
        if (agencyFromMaster.website) {
          lookupCompanyObj.orWhere('agency_website', agencyFromMaster.website)  
        }

        lookupCompanyObj.then(function(matchedCompanies) {
            if (!matchedCompanies.length) return eachSeriesCb()

            //found a matched company! lets update the master list
            var matchedCompany = matchedCompanies[0]

            console.log('updating matched company - ' + matchedCompany.name)

            async.parallel([
              function updateBasicInformation(parallelCb) {
                var updateData = {}

                //website, associations, number_of_offices, number_of_staff
                if (!agencyFromMaster.website && matchedCompany.agency_website) {
                  updateData.website = matchedCompany.agency_website
                }

                if (!agencyFromMaster.associations && matchedCompany.associations) {
                  updateData.associations = matchedCompany.associations
                }

                if (!agencyFromMaster.number_of_offices && matchedCompany.number_of_offices) {
                  updateData.number_of_offices = matchedCompany.number_of_offices
                }

                if (!agencyFromMaster.number_of_staff && matchedCompany.number_of_staff) {
                  updateData.number_of_staff = matchedCompany.number_of_staff
                }

                if (_.keys(updateData).length) {
                  knex('agencies_global_master')
                    .where('id', agencyFromMaster.id)
                    .update(updateData)
                    .then(function() {
                      parallelCb()
                    })
                } else {
                  parallelCb()
                }
              },
              function updateFromCompanyLocale(parallelCb) {

                // get one row from the company locale
                knex
                  .from('company_locale')
                  .where('company_id', matchedCompany.company_id)
                  .then(function(matchedCompanyLocales) {
                    //arbitrarily grab the first one
                    var matchedCompanyLocale = matchedCompanyLocales[0]
                    var updateData = {}

                    // market, country, city, region, phone_number, company_street_address
                    if (!agencyFromMaster.market && matchedCompanyLocale.market) {
                      updateData.market = matchedCompanyLocale.market
                    }

                    if (!agencyFromMaster.country && matchedCompanyLocale.country) {
                      updateData.country = matchedCompanyLocale.country
                    }

                    if (!agencyFromMaster.city && matchedCompanyLocale.city) {
                      updateData.city = matchedCompanyLocale.city
                    }

                    if (!agencyFromMaster.region && matchedCompanyLocale.region) {
                      updateData.region = matchedCompanyLocale.region
                    }

                    if (!agencyFromMaster.phone_number && matchedCompanyLocale.phone_number) {
                      updateData.phone_number = matchedCompanyLocale.phone_number
                    }

                    if (!agencyFromMaster.company_street_address && matchedCompanyLocale.company_street_address) {
                      updateData.company_street_address = matchedCompanyLocale.company_street_address
                    }

                    if (_.keys(updateData).length) {
                      knex('agencies_global_master')
                        .where('id', agencyFromMaster.id)
                        .update(updateData)
                        .then(function() {
                          parallelCb()
                        })
                    } else {
                      parallelCb()
                    }
                  })
              },
              function updatePrimaryContact(parallelCb) {
                //get primary contacts of the matchedcompany
                knex
                  .from('company_contact')
                  .where('company_id', matchedCompany.company_id)
                  .andWhere('type', 'Primary')
                  .then(function(matchedCompanyContacts) {
                    if (matchedCompanyContacts.length) {
                      //arbitrarily choose the first one
                      var matchedCompanyContact = matchedCompanyContacts[0]
                      var updateData = {}

                      if ((!agencyFromMaster.main_contact && !agencyFromMaster.email) && (matchedCompanyContact.first_name && matchedCompanyContact.email)) {
                        updateData.main_contact = matchedCompanyContact.first_name
                        updateData.last_name = matchedCompanyContact.last_name
                        updateData.email = matchedCompanyContact.email
                        updateData.role = matchedCompanyContact.role
                        updateData.phone_number = matchedCompanyContact.phone
                      }

                      if (_.keys(updateData).length) {
                        knex('agencies_global_master')
                          .where('id', agencyFromMaster.id)
                          .update(updateData)
                          .then(function() {
                            parallelCb()
                          })
                      } else {
                        parallelCb()
                      }

                    } else {
                      parallelCb()
                    }
                  })
              },
              function updateSecondaryContact(parallelCb) {
                //get primary contacts of the matchedcompany
                knex
                  .from('company_contact')
                  .where('company_id', matchedCompany.company_id)
                  .andWhere('type', 'Secondary')
                  .then(function(matchedCompanyContacts) {
                    if (matchedCompanyContacts.length) {
                      //arbitrarily choose the first one
                      var matchedCompanyContact = matchedCompanyContacts[0]
                      var updateData = {}

                      if ((!agencyFromMaster.secondary_contact && !agencyFromMaster.secondary_email) && (matchedCompanyContact.first_name && matchedCompanyContact.email)) {
                        updateData.secondary_contact = matchedCompanyContact.first_name
                        updateData.secondary_last_name = matchedCompanyContact.last_name
                        updateData.secondary_email = matchedCompanyContact.email
                        updateData.secondary_role = matchedCompanyContact.role
                      }

                      if (_.keys(updateData).length) {
                        knex('agencies_global_master')
                          .where('id', agencyFromMaster.id)
                          .update(updateData)
                          .then(function() {
                            parallelCb()
                          })
                      } else {
                        parallelCb()
                      }

                    } else {
                      parallelCb()
                    }
                  })
              },
              function updateTertiaryContact(parallelCb) {
                //get primary contacts of the matchedcompany
                knex
                  .from('company_contact')
                  .where('company_id', matchedCompany.company_id)
                  .andWhere('type', 'Tertiary')
                  .then(function(matchedCompanyContacts) {
                    if (matchedCompanyContacts.length) {
                      //arbitrarily choose the first one
                      var matchedCompanyContact = matchedCompanyContacts[0]
                      var updateData = {}

                      if ((!agencyFromMaster.tertiary_contact && !agencyFromMaster.tertiary_email) && (matchedCompanyContact.first_name && matchedCompanyContact.email)) {
                        updateData.tertiary_contact = matchedCompanyContact.first_name
                        updateData.tertiary_last_name = matchedCompanyContact.last_name
                        updateData.tertiary_email = matchedCompanyContact.email
                        updateData.tertiary_role = matchedCompanyContact.role
                      }

                      if (_.keys(updateData).length) {
                        knex('agencies_global_master')
                          .where('id', agencyFromMaster.id)
                          .update(updateData)
                          .then(function() {
                            parallelCb()
                          })
                      } else {
                        parallelCb()
                      }

                    } else {
                      parallelCb()
                    }
                  })
              }
            ], eachSeriesCb)

          })


      }, seriesCb)
    })
  },
  function addNewMasterAgencies(seriesCb) {
    console.log('adding new master agencies...')
    //get all companies
    knex.from('company').then(function(companies) {
      async.eachSeries(companies, function(company, eachSeriesCb) {
        
        //check if the company exists in the master list
        var lookupObject = knex
          .from('agencies_global_master')
          .where('name', company.name)
          .orWhere('other_name', company.name)

        if (company.agency_website) {
          lookupObject.orWhere('website', company.agency_website)
        }

        lookupObject.then(function(agenciesFromGlobalMaster) {
            //if it already exists, just skip
            if (agenciesFromGlobalMaster.length) {
              // console.log('already found master agency - ' + company.name + ' ... skipping')
              return eachSeriesCb()
            }

            console.log('adding new master agency - ' + company.name)

            //not found, we'll insert a new company to the master list
            async.waterfall([
              function getCompanyLocaleInformation(waterfallCb) {
                var data = {}

                knex
                  .from('company_locale')
                  .where('company_id', company.company_id)
                  .then(function(companyLocales) {
                    if (!companyLocales.length) return waterfallCb(null, data)

                    //arbitratily choose first one
                    var companyLocale = companyLocales[0]

                    data.market = companyLocale.market
                    data.country = companyLocale.country
                    data.city = companyLocale.city
                    data.region = companyLocale.region
                    data.company_street_address = companyLocale.company_street_address

                    waterfallCb(null, data)
                  })
              },
              function getCompanyContactInformation(data, waterfallCb) {
                knex
                  .from('company_contact')
                  .where('company_id', company.company_id)
                  .then(function(companyContacts) {
                    if (!companyContacts.length) return waterfallCb(null, data)

                    //grab primary contact
                    var primaryContact = _.findWhere(companyContacts, {type: 'Primary'})
                    if (primaryContact) {
                      data.phone_number = primaryContact.phone
                      data.main_contact = primaryContact.first_name
                      data.last_name = primaryContact.last_name
                      data.email = primaryContact.email
                      data.role = primaryContact.role
                    }

                    //grab secondary contact
                    var secondaryContact = _.findWhere(companyContacts, {type: 'Secondary'})
                    if (secondaryContact) {
                      data.secondary_contact = secondaryContact.first_name
                      data.secondary_last_name = secondaryContact.last_name
                      data.secondary_email = secondaryContact.email
                      data.secondary_role = secondaryContact.role
                    }

                    //grab tertiary contact
                    var tertiaryContact = _.findWhere(companyContacts, {type: 'Tertiary'})
                    if (tertiaryContact) {
                      data.tertiary_contact = tertiaryContact.first_name
                      data.tertiary_last_name = tertiaryContact.last_name
                      data.tertiary_email = tertiaryContact.email
                      data.tertiary_role = tertiaryContact.role
                    }

                    waterfallCb(null, data)
                  })
              },
              function insertNewMasterAgency(data, waterfallCb) {
                //merge in main information
                _.merge(data, {
                  name: company.name,
                  website: company.agency_website,
                  associations: company.associations,
                  number_of_offices: company.number_of_offices,
                  number_of_staff: company.number_of_staff
                })

                knex
                  .insert(data)
                  .into('agencies_global_master')
                  .then(function() {
                    waterfallCb()
                  })
              }
            ], eachSeriesCb)
          })

      }, seriesCb)
    })
  }
], function() {
  console.log('finished processing... fuck yeah')
})







  /*eslint-enable */