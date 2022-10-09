const fs = require('fs');
const puppeteer = require("puppeteer");
const { parse } = require("csv-parse");

const { initializeApp, applicationDefault } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');

initializeApp({
    credential: applicationDefault(),
    databaseURL: 'https://openfair-app-default-rtdb.firebaseio.com',
});
const db = getFirestore();

const LISTINGID_DIGIT_COUNT = 7;
const HISTOGRAM_STEP_COUNT = 50;

const META_COLLECTION_NAME = 'meta_v3';
const META_DOCUMENT_NAME = 'meta';
const OPENFAIR_COLLECTION_NAME = 'openfair_prod_v3';

const METAPROVINCE_COLLECTION_NAME = 'meta_province_v3';
const METACITY_COLLECTION_NAME = 'meta_city_v3';
const METAINDUSTRY_COLLECTION_NAME = 'meta_industry_v3';
const HISTOGRAM_PRICE_COLLECTION_NAME = 'hist_price_v3';
const HISTOGRAM_REVENUE_COLLECTION_NAME = 'hist_revenue_v3';
const HISTOGRAM_PROFIT_COLLECTION_NAME = 'hist_profit_v3';
const HISTOGRAM_BS_COLLECTION_NAME = 'hist_bs_v3';
const HISTOGRAM_META_COLLECTION_NAME = 'hist_meta_v3';

const canadaCities = [];
const canadaProvinces = [];

const hasProvince = (province_id, province_name) => {
    const length = canadaProvinces.length;
    for (let i = 0; i < length; i++) {
        const canadaProvince = canadaProvinces[i];
        if (canadaProvince['province_id'] == province_id && canadaProvince['province_name'] == province_name) return true;
    }
    return false;
}

fs.createReadStream("./data/canadacities.csv")
.pipe(parse({ delimiter: ",", from_line: 2 }))
.on("data", function (row) {
    canadaCities.push({
        'city': row[0],
        'city_ascii': row[1],
        'province_id': row[2],
        'province_name': row[3],
    });

    if (!hasProvince(row[2], row[3])) {
        canadaProvinces.push({
            'province_id': row[2],
            'province_name': row[3],
        });
    }
})
.on("end", function () {
    console.log('Finished to read the csv file');
})
.on("error", function (error) {
    console.log(error.message);
});

const pad = (num, size) => {
    num = num.toString();
    while (num.length < size) num = "0" + num;
    return num;
}

const scraping = async () => {
    const categories = [
        {
            'name': 'Agriculture',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC105-CT10500-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 2,
        },
        {
            'name': 'Automobile',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC110-CT11000-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 3,
        },
        {
            'name': 'Commercial real-estate',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC163-CT16300-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 6,
        },
        {
            'name': 'Communications',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC125-CT12500-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 1,
        },
        {
            'name': 'Construction',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC130-CT13000-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 3,
        },
        {
            'name': 'Education',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC135-CT13500-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 1,
        },
        {
            'name': 'Finance and insurance',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC150-CT15000-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 1,
        },
        {
            'name': 'Health and social services',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC180-CT18000-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 2,
        },
        {
            'name': 'High technology',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC155-CT15500-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 1,
        },
        {
            'name': 'Insolvency',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC164-CT16400-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 0,
        },
        {
            'name': 'Internet',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC165-CT16500-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 1,
        },
        {
            'name': 'Accommodations and restaurants',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC160-CT16000-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 12,
        },
        {
            'name': 'Maintenance and cleaning',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC140-CT14000-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 2,
        },
        {
            'name': 'Manufacturing/Transformation',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC145-CT14500-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 5,
        },
        {
            'name': 'Personal and residential services',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC190-CT19000-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 3,
        },
        {
            'name': 'Professional and technical services',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC185-CT18500-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 2,
        },
        {
            'name': 'Renting',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC170-CT17000-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 1,
        },
        {
            'name': 'Retail business',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC115-CT11500-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 10,
        },
        {
            'name': 'Shows and recreation',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC195-CT19500-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 1,
        },
        {
            'name': 'Tourism',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC196-CT19600-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 1,
        },
        {
            'name': 'Transportation and storage',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC198-CT19800-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 1,
        },
        {
            'name': 'Wholesale business',
            'url': 'https://www.acquizition.biz/prod/trx?r=BS&b=CNCAN-PR...-RG....-SC120-CT12000-ST1&a=&c=LGc-SO.-PS020-PN000',
            'pageCount': 2,
        },
    ];

    try {
        const browser = await puppeteer.launch();
        const page = await browser.newPage();

        page.setDefaultNavigationTimeout(0);

        await page.goto('https://www.acquizition.biz/prod/trx?r=UH&g=e', {
            waitUntil: 'load',
        });

        console.log('111');

        // Login
        await page.type("[name=UP_MAIL]", "mohammad.o@openfairmarket.com")
        await page.type("[name=US_PSWD]", "Mohammad@500")
        await page.select('[name=US_REGN]', 'CON')
        await page.click('[name=button1]');
        await page.waitForNavigation({
            waitUntil: 'load',
        });
        // Login end

        console.log('222');

        let result = [];
        let nCount = 0;

        for (let catIndex = 0; catIndex < categories.length; catIndex++) {
            const category = categories[catIndex];
            console.log('scraping - ', category['name']);

            const nPageCount = category['pageCount'];
            for (let index = 0; index < nPageCount; index++) {
                let baseUrl = category['url'];
                if (index >= 9) baseUrl = baseUrl.substring(0, baseUrl.length - 1);

                const listUrl = `${baseUrl}${index + 1}`;
                await page.goto(listUrl, {
                    waitUntil: 'load',
                });
        
                const aTagHrefs = await page.evaluate(() => {
                    const elements = document.querySelectorAll(".ls-name a, .ls-name2 a");
                    return Array.from(elements).map(e => e.href);
                });
                
                for (let i = 0; i < aTagHrefs.length; i++) {
                    const href = aTagHrefs[i];
                    if (!href.startsWith('javascript:goListing(')) continue;
        
                    const params = href.substring(href.indexOf('(') + 1, href.indexOf(')'));
                    const tokens = params.split(',');
                    if (tokens.length != 3) continue;
        
                    const listingId = tokens[0].replaceAll('\'', '');
                    const listingPos = tokens[1].replaceAll('\'', '');
                    const url = `${listUrl}-IDS${listingPos}0000${listingId}`;
        
                    await page.goto(url, {
                        waitUntil: 'load',
                    });
        
                    const data = await page.evaluate((canadaCities) => {
                        const capitalizeFirstLetter = (string) => string.charAt(0).toUpperCase() + string.slice(1);

                        const getCityAndProvince = (strRegion) => {
                            let city = '';
                            let province_id = '';
                            let province_name = '';
        
                            if (strRegion.length === 0) {
                                return {
                                    'city': city,
                                    'province_id': province_id,
                                    'province_name': province_name,
                                }
                            }

                            strRegion = strRegion.split('\n')[0];
                            
                            let strProvince = '';
                            let nIndex = strRegion.indexOf('-');
                            if (nIndex == -1) {
                                strProvince = strRegion.trim();
                            } else {
                                strProvince = strRegion.substring(0, nIndex).trim();
                                city = strRegion.substring(nIndex + 1).trim();
                                nIndex = city.indexOf(' Area');
                                if (nIndex != -1) city = city.substring(0, nIndex).trim();
                            }

                            strProvince = strProvince.toLowerCase();

                            const cityLength = canadaCities.length;
                            for (let i = 0; i < cityLength; i++) {
                                const canadaCity = canadaCities[i];
                                if (canadaCity['province_id'].toLowerCase() === strProvince || canadaCity['province_name'].toLowerCase() === strProvince) {
                                    province_id = canadaCity['province_id'];
                                    province_name = canadaCity['province_name'];
                                    break;
                                }
                            }

                            if (city.includes('entire province')) city = '';

                            return {
                                'city': city.length == 0 ? '' : capitalizeFirstLetter(city),
                                'province_id': province_id,
                                'province_name': province_name,
                            }
                        };
        
                        const numberFromMoneyString = (strMoney) => {
                            strMoney = strMoney.replaceAll(/\s+/g, '');
                            strMoney = strMoney.replaceAll('$', '');
                            match = strMoney.match('[()a-zA-Z]');
                            if (match) {
                                strMoney = strMoney.substring(0, match.index);
                            }
                            return strMoney == '' ? 0 : Number(strMoney);
                        }

                        const numberFromEmployeeString = (strEmployee) => {
                            let nResult = 0;
                            const tokens = strEmployee.split(' - ');
                            for (let i = 0; i < tokens.length; i++) {
                                let token = tokens[i].trim();
                                const nPos = token.indexOf(' ');
                                if (nPos == -1) continue;
                                const nNumber = Number(token.substring(0, nPos).trim());
                                if (!Number.isNaN(nNumber)) nResult += nNumber;
                            }

                            return nResult;
                        }
                        
                        let tokens = [];
                        let match = undefined;
        
                        let _listing_id = '';
                        let _business_name = document.querySelector(".ls-titre").innerText;
                        _business_name = _business_name.trim();
                        tokens = _business_name.split('\n');
                        if (tokens.length > 1) {
                            const foundIndex = tokens[1].indexOf('-');
                            _listing_id = tokens[1].substring(0, foundIndex).trim();
                            _business_name = tokens[1].substring(foundIndex + 1).trim();
                        }
        
                        const ld_labels = Array.from(document.querySelectorAll('.ld-label'));
        
                        const region_labels = ld_labels.filter(el => el.textContent.includes('Region'));
                        let _region = '';
                        if (region_labels.length) {
                            _region = region_labels[0].parentNode.children[1].innerText;
                        }
                        const cityAndProvince = getCityAndProvince(_region.trim());
        
                        const country_labels = ld_labels.filter(el => el.textContent.includes('Country'));
                        let _country = '';
                        if (country_labels.length) {
                            _country = country_labels[0].parentNode.children[1].innerText;
                            _country = _country.trim();
                        }
        
                        const year_labels = ld_labels.filter(el => el.textContent.includes('Year founded'));
                        let _foundedYear = 0;
                        if (year_labels.length) {
                            _foundedYear = year_labels.length == 0 ? '' : year_labels[0].parentNode.children[1].innerText;
                            _foundedYear = _foundedYear.trim();
                            _foundedYear = _foundedYear == '' ? 0 : Number(_foundedYear);
                        }
        
                        const ld_photos = Array.from(document.querySelectorAll('.ld-photo img'));
                        let _img = [];
                        if (ld_photos.length) {
                            _img = ld_photos.map(el => el.src);
                        } else {
                            const img_labels = ld_labels.filter(el => el.textContent.includes('img src='));
                            if (img_labels.length) {
                                _img.push(img_labels[0].children[0].src);
                            }
                        }
        
                        const businessSector_labels = ld_labels.filter(el => el.textContent.includes('Business sector'));
                        let _industry = [];
                        if (businessSector_labels.length) {
                            const strIndustries = businessSector_labels[0].parentNode.children[1].innerText;
                            const _industries = strIndustries.split('\n');
                            _industry = _industries.map(e => e.substring(0, e.indexOf(' - ')).trim().replaceAll('/', ' & '));
                            _industry = [...new Set(_industry)];
                        }
        
                        let _l_description = document.querySelector(".ld-desc").innerText;
                        _l_description = _l_description.trim();
        
                        const employee_labels = ld_labels.filter(el => el.textContent.includes('Number of employees'));
                        let _numberOfEmployees = 0;
                        if (employee_labels.length) {
                            _numberOfEmployees = employee_labels[0].parentNode.children[1].innerText;
                            _numberOfEmployees = numberFromEmployeeString(_numberOfEmployees.trim());
                        }
        
                        const price_labels = ld_labels.filter(el => el.textContent.includes('Selling price'));
                        let _price = 0;
                        if (price_labels.length) {
                            _price = price_labels[0].parentNode.children[1].innerText;
                            _price = numberFromMoneyString(_price.trim());
                        }
        
                        const profit_labels = ld_labels.filter(el => el.textContent.includes('Profit'));
                        let _profit = 0;
                        if (profit_labels.length) {
                            _profit = profit_labels[0].parentNode.children[1].innerText;
                            _profit = numberFromMoneyString(_profit.trim());
                        }
        
                        const revenue_labels = ld_labels.filter(el => el.textContent.includes('revenue'));
                        let _revenue = 0;
                        if (revenue_labels.length) {
                            _revenue = revenue_labels[0].parentNode.children[1].innerText;
                            _revenue = numberFromMoneyString(_revenue.trim());
                        }
        
                        const _s_description = _l_description.length > 200 ? _l_description.substring(0, 200) + '...' : _l_description;
        
                        const sales_labels = ld_labels.filter(el => el.textContent.includes('sale'));
                        let _sales = '';
                        if (sales_labels.length) {
                            _sales = sales_labels[0].parentNode.children[1].innerText;
                            _sales = _sales.trim();
                            if (_sales.startsWith('$')) {
                                _sales = numberFromMoneyString(_sales);
                            }
                        }
        
                        const pr_labelrs = Array.from(document.querySelectorAll('.pr-labelr'));
        
                        const email_labels = pr_labelrs.filter(el => el.textContent.includes('E-mail'));
                        let _seller_email = '';
                        if (email_labels.length) {
                            _seller_email = email_labels[0].parentNode.children[1].innerText;
                            _seller_email = _seller_email.trim();
                        }
        
                        const firstname_labels = pr_labelrs.filter(el => el.textContent.includes('First name'));
                        let _firstname = '';
                        if (firstname_labels.length) {
                            _firstname = firstname_labels[0].parentNode.children[1].innerText;
                            _firstname = _firstname.trim();
                        }
                        const lastname_labels = pr_labelrs.filter(el => el.textContent.includes('Last name'));
                        let _lastname = '';
                        if (lastname_labels.length) {
                            _lastname = lastname_labels[0].parentNode.children[1].innerText;
                            _lastname = _lastname.trim();
                        }
                        let _seller_name = _firstname;
                        if (_lastname.length != 0) {
                            _seller_name += ' ' + _lastname;
                        }
        
                        const phone_labels = pr_labelrs.filter(el => el.textContent.includes('Phone number'));
                        let _seller_phone_number = '';
                        if (phone_labels.length) {
                            _seller_phone_number = phone_labels[0].parentNode.children[1].innerText;
                            _seller_phone_number = _seller_phone_number.trim();
                        }
        
                        const phone2_labels = pr_labelrs.filter(el => el.textContent.includes('Other phone'));
                        let _seller_phone_number_2 = '';
                        if (phone2_labels.length) {
                            _seller_phone_number_2 = phone2_labels[0].parentNode.children[1].innerText;
                            _seller_phone_number_2 = _seller_phone_number_2.trim();
                        }
        
                        return {
                            'business_name': _business_name,
                            'city': cityAndProvince['city'],
                            'province_id': cityAndProvince['province_id'],
                            'province_name': cityAndProvince['province_name'],
                            'country': _country,
                            "currency":"CAD",
                            'date_scraped': (new Date()).toUTCString(),
                            'f_business_name': _business_name,
                            'f_l_description': _l_description,
                            'f_s_description': _s_description,
                            'foundedYear': _foundedYear,
                            'images': _img,
                            'img': _img,
                            'industry': _industry,
                            'l_description': _l_description,
                            'listing_created_date': (new Date()).toUTCString(),
                            'listing_id': _listing_id,
                            'listing_url': document.URL,
                            'numOfViews': 0,
                            'numberOfEmployees': _numberOfEmployees,
                            'of_listing_id': '',
                            'price': _price,
                            'profit': _profit,
                            'revenue': _revenue,
                            's_description': _s_description,
                            'sales': _sales,
                            'seller_email': _seller_email,
                            'seller_name': _seller_name,
                            'seller_phone_number': _seller_phone_number,
                            'seller_phone_number_2': _seller_phone_number_2,
                            "source":"acquizition.biz",
                            "source_url":"https://www.acquizition.biz/",
                        };
                    }, canadaCities);
        
                    result.push(data);
                    nCount++;
                    console.log('Done - ', nCount);
                }
            }
        }

        // fs.writeFile('test.txt', JSON.stringify(result, null, "\t"), err => {
        //     if (err) {
        //         console.error(err);
        //     } else {
        //         console.log('File write successful.');
        //     }
        // });

        await browser.close();

        console.log(`Saving ${result.length} results to ${OPENFAIR_COLLECTION_NAME}...`);
        const docs = (await db.collection(OPENFAIR_COLLECTION_NAME).get()).docs;
        const collectionData = docs.map(d => d.data());
        await buildCollections(result, collectionData);

        console.log('Finished');
    } catch (error) {
        console.log(error);
    }
}

const findIndexForBusinessName = (strBusinessName, collectionData) => {
    for (let i = 0; i < collectionData.length; i++) {
        if (collectionData[i]['business_name'] == strBusinessName) return i;
    }

    return -1;
}

const buildCollections = async (result, collectionData) => {
    const metaReference = db.collection(META_COLLECTION_NAME).doc(META_DOCUMENT_NAME);
    const metaSnapshot = metaReference.get();
    const metaData = (await metaSnapshot).data();
    let lastNumId = metaData ? metaData['last_num_id'] : 0;
    let listingId;

    const collection = db.collection(OPENFAIR_COLLECTION_NAME);

    let bDirty = false;
    const nResultLength = result.length;
    for (let i = 0; i < nResultLength; i++) {
        const data = result[i];

        const nFoundIndex = findIndexForBusinessName(data['business_name'], collectionData);
        if (nFoundIndex != -1) continue;

        bDirty = true;
        lastNumId++;
        listingId = `OF${pad(lastNumId, LISTINGID_DIGIT_COUNT)}`;
        data['of_listing_id'] = listingId;
        await collection.doc(listingId).set(data);
        collectionData.push(data);

        console.log('Saved - ', i + 1);
    }

    if (bDirty) {
        await deleteCollection(META_COLLECTION_NAME);
        await metaReference.set({
            count: lastNumId,
            last_id: listingId,
            last_num_id: lastNumId,
        });

        await buildMetaProvince(collectionData);
        await buildMetaCity(collectionData);
        await buildMetaIndustry(collectionData);
        await buildFilter(collectionData);
    }
}

const deleteCollection = async (collectionName) => {
    const batch = db.batch();

    let docs = (await db.collection(collectionName).get()).docs;
    for (let i = 0; i < docs.length; i++) {
        batch.delete(docs[i].ref);
    }

    await batch.commit();
}

const buildMetaProvince = async (collectionData) => {
    const result = [];

    const indexOfProvince = (province_name) => {
        for (let i = 0; i < result.length; i++) {
            if (result[i]['name'] == province_name) return i;
        }

        return -1;
    }
    
    const length = collectionData.length;
    for (let i = 0; i < length; i++) {
        const data = collectionData[i];
        const province = data['province_name'];
        if (province.length == 0) continue;
        
        const nIndex = indexOfProvince(province);
        if (nIndex == -1) {
            result.push({
                'count': 1,
                'image': data['images'].length != 0 ? data['images'][0] : '',
                'name': province,
            });
        } else {
            result[nIndex]['count'] = result[nIndex]['count'] + 1;
            if (result[nIndex]['image'] == '' && data['images'].length) {
                result[nIndex]['image'] = data['images'][0];
            }
        }
    }

    // temp code
    const imageUrls = [
        'https://www.acquizition.biz/prod/photos/A139627-4.jpg',
        'https://www.acquizition.biz/prod/photos/A140129-4.jpg',
        'https://www.acquizition.biz/prod/photos/A139885-6.jpg',
        'https://www.acquizition.biz/prod/photos/A139811-5.jpg',
        'https://www.acquizition.biz/prod/photos/A140048-3.jpg',
        'https://www.acquizition.biz/prod/photos/A140049-4.jpg',
        'https://www.acquizition.biz/prod/photos/A140214-3.jpg',
        'https://www.acquizition.biz/prod/photos/A132070-3.jpg',
        'https://www.acquizition.biz/prod/photos/A132068-3.jpg',
        'https://www.acquizition.biz/prod/photos/A140194-4.jpg',
        'https://www.acquizition.biz/prod/photos/A140165-6.jpg',
        'https://www.acquizition.biz/prod/photos/A140177-6.jpg',
        'https://www.acquizition.biz/prod/photos/A140161-6.jpg',
        'https://www.acquizition.biz/prod/photos/A140121-5.jpg',
        'https://www.acquizition.biz/prod/photos/A140160-6.jpg',
        'https://www.acquizition.biz/prod/photos/A138937-5.jpg',
        'https://www.acquizition.biz/prod/photos/A138937-3.jpg',
        'https://www.acquizition.biz/prod/photos/A140106-5.jpg',
        'https://www.acquizition.biz/prod/photos/A140007-4.jpg',
        'https://www.acquizition.biz/prod/photos/A138187-5.jpg',
    ];

    for (let i = 0; i < result.length; i++) {
        if (result[i]['image'] == '') {
            const imageIndex = Math.floor(Math.random() * imageUrls.length);
            result[i]['image'] = imageUrls[imageIndex];
        }
    }
    // temp code end

    await deleteCollection(METAPROVINCE_COLLECTION_NAME);
    const collection = db.collection(METAPROVINCE_COLLECTION_NAME);
    for (let i = 0; i < result.length; i++) {
        const d = result[i];
        await collection.doc(d['name']).set(d);
    }

    console.log(`Built ${METAPROVINCE_COLLECTION_NAME} successfully`);
}

const buildMetaCity = async (collectionData) => {
    const result = [];

    const indexOfCity = (city_name) => {
        for (let i = 0; i < result.length; i++) {
            if (result[i]['name'] == city_name) return i;
        }

        return -1;
    }
    
    const length = collectionData.length;
    for (let i = 0; i < length; i++) {
        const data = collectionData[i];
        const city = data['city'];
        if (city.length == 0) continue;

        const nIndex = indexOfCity(city);
        if (nIndex == -1) {
            result.push({
                'count': 1,
                'image': data['images'].length != 0 ? data['images'][0] : '',
                'name': city,
            });
        } else {
            result[nIndex]['count'] = result[nIndex]['count'] + 1;
            if (result[nIndex]['image'] == '' && data['images'].length) {
                result[nIndex]['image'] = data['images'][0];
            }
        }
    }

    // temp code
    const imageUrls = [
        'https://www.acquizition.biz/prod/photos/A139627-4.jpg',
        'https://www.acquizition.biz/prod/photos/A140129-4.jpg',
        'https://www.acquizition.biz/prod/photos/A139885-6.jpg',
        'https://www.acquizition.biz/prod/photos/A139811-5.jpg',
        'https://www.acquizition.biz/prod/photos/A140048-3.jpg',
        'https://www.acquizition.biz/prod/photos/A140049-4.jpg',
        'https://www.acquizition.biz/prod/photos/A140214-3.jpg',
        'https://www.acquizition.biz/prod/photos/A132070-3.jpg',
        'https://www.acquizition.biz/prod/photos/A132068-3.jpg',
        'https://www.acquizition.biz/prod/photos/A140194-4.jpg',
        'https://www.acquizition.biz/prod/photos/A140165-6.jpg',
        'https://www.acquizition.biz/prod/photos/A140177-6.jpg',
        'https://www.acquizition.biz/prod/photos/A140161-6.jpg',
        'https://www.acquizition.biz/prod/photos/A140121-5.jpg',
        'https://www.acquizition.biz/prod/photos/A140160-6.jpg',
        'https://www.acquizition.biz/prod/photos/A138937-5.jpg',
        'https://www.acquizition.biz/prod/photos/A138937-3.jpg',
        'https://www.acquizition.biz/prod/photos/A140106-5.jpg',
        'https://www.acquizition.biz/prod/photos/A140007-4.jpg',
        'https://www.acquizition.biz/prod/photos/A138187-5.jpg',
    ];

    for (let i = 0; i < result.length; i++) {
        if (result[i]['image'] == '') {
            const imageIndex = Math.floor(Math.random() * imageUrls.length);
            result[i]['image'] = imageUrls[imageIndex];
        }
    }
    // temp code end

    await deleteCollection(METACITY_COLLECTION_NAME);
    const collection = db.collection(METACITY_COLLECTION_NAME);
    for (let i = 0; i < result.length; i++) {
        const d = result[i];
        await collection.doc(d['name']).set(d);
    }

    console.log(`Built ${METACITY_COLLECTION_NAME} successfully`);
}

const buildMetaIndustry = async (collectionData) => {
    const industries = new Set();

    const length = collectionData.length;
    for (let i = 0; i < length; i++) {
        const data = collectionData[i];
        const industry = data['industry'];
        for (let k = 0; k < industry.length; k++) {
            industries.add(industry[k]);
        }
    }

    const result = [];
    [...industries].forEach(d => {
        result.push({
            'count': 0,
            'image': '',
            'name': d,
        });
    });
    
    const indexOfIndustry = (industry_name) => {
        for (let i = 0; i < result.length; i++) {
            if (result[i]['name'] == industry_name) return i;
        }

        return -1;
    }
    
    for (let i = 0; i < length; i++) {
        const data = collectionData[i];
        const industry = data['industry'];
        for (let k = 0; k < industry.length; k++) {
            const nIndex = indexOfIndustry(industry[k]);
            if (nIndex == -1) continue;
    
            result[nIndex]['count'] = result[nIndex]['count'] + 1;
            if (result[nIndex]['image'] == '' && data['images'].length) {
                result[nIndex]['image'] = data['images'][0];
            }
        }
    }

    await deleteCollection(METAINDUSTRY_COLLECTION_NAME);
    const collection = db.collection(METAINDUSTRY_COLLECTION_NAME);
    for (let i = 0; i < result.length; i++) {
        const d = result[i];
        await collection.doc(d['name']).set(d);
    }

    console.log(`Built ${METAINDUSTRY_COLLECTION_NAME} successfully`);
}

const buildFilter = async (collectionData) => {
    let objPrice = {
        'key': 'price',
        'min': 0,
        'max': 0,
        'sum': 0,
    };

    let objRevenue = {
        'key': 'revenue',
        'min': 0,
        'max': 0,
        'sum': 0,
    }

    let objProfit = {
        'key': 'profit',
        'min': 0,
        'max': 0,
        'sum': 0,
    }

    let objBS = {
        'key': 'business_size',
        'min': 0,
        'max': 0,
        'sum': 0,
    }

    const keys = ['price', 'revenue', 'profit', 'numberOfEmployees'];
    const metaValues = [objPrice, objRevenue, objProfit, objBS];
    const histValues = [];

    const length = collectionData.length;
    for (let i = 0; i < length; i++) {
        const data = collectionData[i];

        for (let k = 0; k < 4; k++) {
            const value = data[keys[k]];
            metaValues[k]['sum'] += value;

            if (metaValues[k]['min'] > value) {
                metaValues[k]['min'] = value;
            }
            if (metaValues[k]['max'] < value) {
                metaValues[k]['max'] = value;
            }
        }
    }

    for (let k = 0; k < 4; k++) {
        metaValues[k]['count'] = length;
        metaValues[k]['avg'] = length == 0 ? 0 : Math.floor(metaValues[k]['sum'] / length);
        metaValues[k]['step'] = Math.ceil((metaValues[k]['max'] - metaValues[k]['min']) / HISTOGRAM_STEP_COUNT);

        const values = [];
        for (let i = metaValues[k]['min']; i <= metaValues[k]['max']; i += metaValues[k]['step']) {
            values.push({
                'min': i,
                'max': i + metaValues[k]['step'],
                'count': 0,
            });
        }
        histValues.push(values);
    }

    const indexOfValue = (value, values) => {
        for (let i = 0; i < values.length; i++) {
            const d = values[i];
            if (value >= d['min'] && value < d['max']) return i;
        }

        return -1;
    }

    for (let i = 0; i < length; i++) {
        const data = collectionData[i];

        for (let k = 0; k < 4; k++) {
            const value = data[keys[k]];

            const nIndex = indexOfValue(value, histValues[k]);
            if (nIndex == -1) continue;
            histValues[k][nIndex]['count'] = histValues[k][nIndex]['count'] + 1;
        }
    }

    await deleteCollection(HISTOGRAM_META_COLLECTION_NAME);
    let collection = db.collection(HISTOGRAM_META_COLLECTION_NAME);
    for (let k = 0; k < 4; k++) {
        await collection.doc(metaValues[k]['key']).set(metaValues[k]);
    }

    const collectionNames = [
        HISTOGRAM_PRICE_COLLECTION_NAME,
        HISTOGRAM_REVENUE_COLLECTION_NAME,
        HISTOGRAM_PROFIT_COLLECTION_NAME,
        HISTOGRAM_BS_COLLECTION_NAME,
    ];

    for (let k = 0; k < 4; k++) {
        await deleteCollection(collectionNames[k]);
        collection = db.collection(collectionNames[k]);
        const values = histValues[k];
        for (let i = 0; i < values.length; i++) {
            const d = values[i];
            await collection.doc(d['min'].toString()).set(d);
        }
    }

    console.log(`Built filter collections successfully`);
}

scraping();
