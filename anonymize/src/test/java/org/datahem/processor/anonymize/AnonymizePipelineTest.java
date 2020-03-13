package org.datahem.processor.anonymize;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */


import io.anemos.metastore.core.proto.*;
import org.datahem.processor.anonymize.AnonymizeStreamPipeline.*;
import org.datahem.processor.utils.ProtobufUtils;
//import org.datahem.processor.anonymize.*;

import com.google.gson.Gson;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;

import org.datahem.processor.utils.ProtobufUtils;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;

import com.google.protobuf.util.JsonFormat;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTimeZone;
import org.joda.time.DateTime;

import org.json.JSONObject;
import org.json.JSONArray;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Optional;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;
import java.util.Base64;
import java.util.Iterator;
import com.google.protobuf.ByteString;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.HashMultimap;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;

import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

import com.google.auto.value.AutoValue;
/*
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
*/

@RunWith(JUnit4.class)
public class AnonymizePipelineTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(AnonymizePipelineTest.class);
	
	@Rule public transient TestPipeline p = TestPipeline.create();

 
    @Test
	public void cleanPubsubMessageTest(){

        String json = "{\"OldImage\":{\"Id\":\"1329928\",\"MemberType\":0,\"SocialSecurityNo\":\"808080-0808\",\"EmailAddress\":\"c.kilefors.paiser@gmail.com\",\"AddressId\":null,\"Address\":{\"Id\":\"7129852\",\"LegacyAddressId\":null,\"MemberId\":\"1329928\",\"AddressType\":\"M\",\"CompanyName\":null,\"FirstName\":\"Lina\",\"LastName\":\"Kilefors Paiser\",\"AddressRow1\":\"Rökvägen 10\",\"AddressRow2\":null,\"DoorCode\":null,\"AddressInfo\":null,\"PostalCode\":\"13755\",\"City\":\"Tullinge\",\"PhoneHome\":null,\"PhoneOther\":null,\"PhoneMobile\":\"+46 76 224 19 44\",\"AlternativeName\":null,\"AlternativePhoneHome\":null,\"AlternativePhoneMobile\":null,\"Latitude\":59.1147,\"Longitude\":18.0728,\"GeoEncoded\":null,\"IsVisible\":true,\"IsValid\":true,\"CreationDate\":\"2019-02-14T21:21:23+00:00\",\"LastModifiedDate\":\"2019-02-14T21:21:24+00:00\"},\"LoginInformation\":{\"LastLoginDate\":\"2019-02-13T16:42:40+00:00\",\"LoginCount\":2,\"LastLoginIp\":\"34.253.89.671\"},\"StoreId\":\"10\",\"NoGiftProducts\":false,\"ReplacementCode\":null,\"Favorites\":null,\"AcceptNewsletter\":false,\"DontSendDeliverySms\":false,\"SendInvoiceReminderSms\":false,\"AllowMonthlyInvoice\":false,\"AllowPayEx\":false,\"DisableFlexPay\":true,\"TotalCreditLimit\":0.0,\"PerPurchaseCreditLimit\":0.0,\"AllowedPaymentTypes\":[4,8,11],\"InvoiceAddressId\":null,\"InvoiceAddress\":null,\"SavedPayExCCPaymentTicket\":null,\"MemberPaymentSettings\":null,\"AllowMultipleOrdersOnSameDay\":false,\"ExtraWorkTime\":0,\"DriverDeliveryNote\":null,\"CanChangeDeliveryAddressForInvoice\":true,\"InvoicePaymentAllowed\":true,\"LockPhoneNumber\":null,\"NumberOfPurchases\":4,\"SumOfAllPurchases\":3385.39,\"TotalCreditGiven\":100.0,\"TotalComplaint\":0,\"LastUsedOrderId\":\"0\",\"AboutMember\":null,\"LogHistory\":null,\"OrderPickingNote\":null,\"MemberGroups\":null,\"MemberDeliveryPass\":null,\"MemberBonus\":{\"MemberId\":1329928,\"BonusId\":6329773,\"BonusCode\":\"C6329770C94807\",\"BonusCheckDate\":\"2019-02-22T00:00:00+00:00\",\"TotalRemainingBonusPoint\":34.0,\"BonusCheckAmountLeft\":21.0},\"MemberRating\":{\"MemberId\":1329928,\"StarRating\":1.0,\"AverageTB2PerOrderScore\":1.0,\"NumberOfOrdersScore\":2.0,\"OrderFrequencyScore\":0.0},\"MemberDiscounts\":[{\"DiscountOfferId\":1122268,\"MemberId\":1329928,\"DiscountUsedDate\":\"2019-02-14T21:21:25+00:00\",\"DiscountCount\":1,\"OrderIds\":null},{\"DiscountOfferId\":1122268,\"MemberId\":1329928,\"DiscountUsedDate\":null,\"DiscountCount\":1,\"OrderIds\":[29395232]}],\"CreationDate\":\"2019-02-13T16:42:40+00:00\",\"LastModifiedDate\":\"2019-05-25T01:33:10+00:00\",\"RemovedDate\":null},\"NewImage\":{\"Id\":\"1329928\",\"MemberType\":0,\"SocialSecurityNo\":null,\"EmailAddress\":\"kilefors.paiser@gmail.comxxx\",\"AddressId\":null,\"Address\":{\"Id\":\"7129852\",\"LegacyAddressId\":null,\"MemberId\":\"1329928\",\"AddressType\":\"M\",\"CompanyName\":null,\"FirstName\":\"Lina\",\"LastName\":\"Kilefors Paiser\",\"AddressRow1\":\"Rökvägen 10\",\"AddressRow2\":null,\"DoorCode\":null,\"AddressInfo\":null,\"PostalCode\":\"13755\",\"City\":\"Tullinge\",\"PhoneHome\":null,\"PhoneOther\":null,\"PhoneMobile\":\"+46 76 224 41 94\",\"AlternativeName\":null,\"AlternativePhoneHome\":null,\"AlternativePhoneMobile\":null,\"Latitude\":59.1147,\"Longitude\":18.0728,\"GeoEncoded\":true,\"IsVisible\":true,\"IsValid\":true,\"CreationDate\":\"2019-02-14T21:21:23+00:00\",\"LastModifiedDate\":\"2019-02-14T21:21:24+00:00\"},\"LoginInformation\":{\"LastLoginDate\":\"2019-02-13T16:42:40+00:00\",\"LoginCount\":2,\"LastLoginIp\":\"34.253.89.676\"},\"StoreId\":\"10\",\"NoGiftProducts\":false,\"ReplacementCode\":null,\"Favorites\":null,\"AcceptNewsletter\":true,\"DontSendDeliverySms\":false,\"SendInvoiceReminderSms\":false,\"AllowMonthlyInvoice\":false,\"AllowPayEx\":false,\"DisableFlexPay\":true,\"TotalCreditLimit\":0.0,\"PerPurchaseCreditLimit\":0.0,\"AllowedPaymentTypes\":[4,8,11],\"InvoiceAddressId\":null,\"InvoiceAddress\":null,\"SavedPayExCCPaymentTicket\":null,\"MemberPaymentSettings\":null,\"AllowMultipleOrdersOnSameDay\":false,\"ExtraWorkTime\":0,\"DriverDeliveryNote\":null,\"CanChangeDeliveryAddressForInvoice\":true,\"InvoicePaymentAllowed\":true,\"LockPhoneNumber\":null,\"NumberOfPurchases\":4,\"SumOfAllPurchases\":3385.39,\"TotalCreditGiven\":100.0,\"TotalComplaint\":0,\"LastUsedOrderId\":\"0\",\"AboutMember\":null,\"LogHistory\":null,\"OrderPickingNote\":null,\"MemberGroups\":null,\"MemberDeliveryPass\":null,\"MemberBonus\":{\"MemberId\":1329928,\"BonusId\":6329773,\"BonusCode\":\"B6329770C94708\",\"BonusCheckDate\":\"2019-02-22T00:00:00+00:00\",\"TotalRemainingBonusPoint\":345.0,\"BonusCheckAmountLeft\":2155.0},\"MemberRating\":{\"MemberId\":1329928,\"StarRating\":1.0,\"AverageTB2PerOrderScore\":1.0,\"NumberOfOrdersScore\":2.0,\"OrderFrequencyScore\":0.0},\"MemberDiscounts\":[{\"DiscountOfferId\":1122268,\"MemberId\":1329928,\"DiscountUsedDate\":\"2019-02-14T21:21:25+00:00\",\"DiscountCount\":1,\"OrderIds\":null},{\"DiscountOfferId\":1122268,\"MemberId\":1329928,\"DiscountUsedDate\":null,\"DiscountCount\":1,\"OrderIds\":[29395732]}],\"CreationDate\":\"2019-02-13T16:42:40+00:00\",\"LastModifiedDate\":\"2020-02-21T09:41:04+00:00\",\"RemovedDate\":null},\"EventType\":null,\"EventId\":\"1cd57aa9-9307-4682-9810-1c8deda0242d\",\"Published\":\"2020-02-21T08:41:08.2810103Z\"}";

        HashMap<String, String> attributes = new HashMap<String,String>();
        attributes.put("topic","member-service-MemberProcessTopic");        
        attributes.put("uuid","24c5797e-4a64-5cee-a1ce-2fb7d380de06");        
        attributes.put("timestamp","2020-02-21T08:41:08.289Z");        
        ByteString bs = ByteString.copyFromUtf8(json);
        byte[] jsonPayload = bs.toByteArray();

        PubsubMessage newPubsubMessage = new PubsubMessage(jsonPayload, attributes);
        
        ValueProvider<String> bucketName = p.newProvider("mathem-ml-datahem-test-descriptor");
        ValueProvider<String> fileDescriptorName = p.newProvider("schemas.desc");
        ValueProvider<String> descriptorFullName = p.newProvider("mathem.commerce.member_service.member.v2.Member");

        try{
            LOG.info("ok ");
            PCollection<PubsubMessage> output = p
                .apply(Create.of(Arrays.asList(newPubsubMessage)))
                .apply("PubsubMessage to TableRow", ParDo.of(new CleanPubsubMessageFn(
                    bucketName,
                    fileDescriptorName,
                    descriptorFullName
                )));

            //PAssert.that(output).containsInAnyOrder(assertTableRow);
            p.run();
            LOG.info("withoutOptionsTest assert TableRow without errors.");
        }catch (Exception e) {
            LOG.info("error");
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}