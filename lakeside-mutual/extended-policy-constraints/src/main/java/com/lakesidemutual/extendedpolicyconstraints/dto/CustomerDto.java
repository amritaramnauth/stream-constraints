package com.lakesidemutual.extendedpolicyconstraints.dto;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import org.springframework.hateoas.RepresentationModel;

/**
 * The CustomerDto class is a data transfer object (DTO) that represents a single customer.
 * It inherits from the ResourceSupport class which allows us to create a REST representation (e.g., JSON, XML)
 * that follows the HATEOAS principle. For example, links can be added to the representation (e.g., self, address.change)
 * which means that future actions the client may take can be discovered from the resource representation.
 *
 * The CustomerDto is part of the API contract (see the getCustomer() method in @CustomerInformationHolder). In the Swagger API docs
 * (http://localhost:8090/v2/api-docs) generated by SpringFox the is represented by the following JSON Schema. JSON Schema (https://json-schema.org/)
 * is an IETF draft to define the schema of JSON documents:
 *
 * <pre>
 * <code>
 * {
 *  "title":"CustomerDto",
 *  "type":"object",
 *  "properties":{
 *     // Note that this _links section of the JSON Schema appears here, because CustomerDto
 *     // inherits from ResourceSupport which can add HATEOAS links to the JSON representation
 *     // of the resource.
 *     "_links":{
 *        "type":"array",
 *        "xml":{
 *           "name":"link",
 *           "attribute":false,
 *           "wrapped":false
 *        },
 *        "items":{
 *           "$ref":"#/definitions/Link"
 *        }
 *     },
 *     "birthday":{
 *        "type":"string",
 *        "format":"date-time"
 *     },
 *     "city":{
 *        "type":"string"
 *     },
 *     "customerId":{
 *        "type":"string"
 *     },
 *     "email":{
 *        "type":"string"
 *     },
 *     "firstname":{
 *        "type":"string"
 *     },
 *     "lastname":{
 *        "type":"string"
 *     },
 *     "moveHistory":{
 *        "type":"array",
 *        "items":{
 *           "$ref":"#/definitions/AddressDto"
 *        }
 *     },
 *     "phoneNumber":{
 *        "type":"string"
 *     },
 *     "postalCode":{
 *        "type":"string"
 *     },
 *     "streetAddress":{
 *        "type":"string"
 *     }
 *  }
 * }
 * </code>
 * </pre>
 *
 * @see <a href="https://docs.spring.io/spring-hateoas/docs/current/reference/html/">Spring HATEOAS - Reference Documentation</a>
 */
public class CustomerDto extends RepresentationModel {
	private String customerId;
	@JsonUnwrapped
	private CustomerProfileDto customerProfile;

	public CustomerDto() {
	}

	public CustomerDto(String customerId, CustomerProfileDto customerProfile) {
		this.customerId = customerId;
		this.customerProfile = customerProfile;
	}

	public String getCustomerId() {
		return customerId;
	}

	public CustomerProfileDto getCustomerProfile() {
		return this.customerProfile;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public void setCustomerProfile(CustomerProfileDto customerProfile) {
		this.customerProfile = customerProfile;
	}
}
