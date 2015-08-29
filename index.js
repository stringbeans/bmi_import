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

var bmiMasterIdsToSkip = []

//get all the rows in the database
knex.from('bmi_master').then(function(bmiRows) {

  //foreach row...
  async.eachSeries(bmiRows, function(bmiRow, cb) {

    if (_.indexOf(bmiMasterIdsToSkip, bmiRow.id) !== -1) return cb()

    async.waterfall([
      function findDupes(waterfallCb) {
        //find all the matching rows based on the agency_name, main_email, or agency_website
        knex
          .from('bmi_master')
          .where('agency_name', bmiRow.agency_name)
          .orWhere('main_email', bmiRow.main_email)
          .orWhere('agency_website', bmiRow.agency_website)
          .andWhere('id', '!=', bmiRow.id)
          .then(function(dupes) {

            if (dupes.length) {
              //store the list of id's that matched (so we can skip them later)
              bmiMasterIdsToSkip = bmiMasterIdsToSkip.concat(_.pluck(dupes, 'id'))
            } 
            
            return waterfallCb(null, dupes)
          })
      },
      function addToNewTables(dupes, waterfallCb) {
        //add the matching row, with its copies, into a different table...

        //insert into company
        knex
          .insert({
            company_id: bmiRow.company_id,
            name: bmiRow.agency_name,
            agency_website: bmiRow.agency_website,
            website_email: bmiRow.website_email,
            associations: bmiRow.associations,
            number_of_offices: bmiRow.number_of_offices,
            number_of_staff: bmiRow.number_of_staff
          })
          .into('company')
          .then(function(resp) {
            var companyId = resp[0]

            if (dupes.length) {
              async.parallel([
                function insertIntoCompanyLocale(parallelCb) {

                  async.eachSeries(dupes, function(dupe, eachSeries) {
                    knex
                      .from('company_locale')
                      .where('company_id', companyId)
                      .andWhere('market', dupe.market)
                      .andWhere('country', dupe.country)
                      .andWhere('city', dupe.city)
                      .andWhere('region', dupe.region)
                      .then(function(companyLocalDupes) {
                        //if there are no company local dupes, then lets go ahead and add
                        if (companyLocalDupes.length) {
                          return eachSeries()
                        } else {
                          knex
                            .insert({
                              company_id: companyId,
                              market: dupe.market,
                              country: dupe.country,
                              city: dupe.city,
                              region: dupe.region,
                              phone_number: dupe.phone_number,
                              company_street_address: dupe.company_street_address,
                              fax_number: dupe.fax_number
                            })
                            .into('company_locale')
                            .then(function() {
                              return eachSeries()
                            })
                        }
                      })
                  }, parallelCb)

                },
                function insertIntoCompanyContact(parallelCb) {

                  async.eachSeries(dupes, function(dupe, eachSeries) {

                    knex
                      .from('company_contact')
                      .where('company_id', companyId)
                      .andWhere('email', dupe.main_email)
                      .orWhere('email', dupe.second_contact_email)
                      .orWhere('email', dupe.tertiary_email)
                      .then(function(companyContactDupes) {
                        if (companyContactDupes.length) {
                          return eachSeries()
                        } else {

                          async.parallel([
                            function insertPrimaryCompanyContact(companyContactParallelCb) {
                              if (!dupe.main_contact) return companyContactParallelCb()

                              knex
                                .insert({
                                  company_id: companyId,
                                  type: 'Primary',
                                  first_name: dupe.main_contact,
                                  last_name: dupe.main_last_name,
                                  email: dupe.main_email,
                                  role: dupe.main_role,
                                  phone: dupe.main_Phone,
                                })
                                .into('company_contact')
                                .then(function(){return companyContactParallelCb()})
                            },
                            function insertSecondaryCompanyContact(companyContactParallelCb) {
                              if (!dupe.second_contact_full_name) return companyContactParallelCb()

                              knex
                                .insert({
                                  company_id: companyId,
                                  type: 'Secondary',
                                  first_name: dupe.second_contact_full_name,
                                  last_name: dupe.second_last_name,
                                  email: dupe.second_contact_email,
                                  role: dupe.second_role,
                                  phone: dupe.second_phone,
                                })
                                .into('company_contact')
                                .then(function(){return companyContactParallelCb()})
                            },
                            function insertTertiaryCompanyContact(companyContactParallelCb) {
                              if (!dupe.tertiary_name) return companyContactParallelCb()

                              knex
                                .insert({
                                  company_id: companyId,
                                  type: 'Tertiary',
                                  first_name: dupe.tertiary_name,
                                  last_name: dupe.tertiary_last_name,
                                  email: dupe.tertiary_email,
                                  role: dupe.tertiary_role,
                                  phone: dupe.tertiary_phone,
                                })
                                .into('company_contact')
                                .then(function(){return companyContactParallelCb()})
                            }
                          ], eachSeries)
                        }
                      })

                  }, parallelCb)
                }
              ], waterfallCb)
            } else {
              return waterfallCb()
            }
          })
      }
    ], cb)

  }, function(err) {
    console.log("processing done... fuck yeah")
  })

    

})





/*eslint-enable */
